#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'

function print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --canary | --dev          Specify the environment (canary or dev)"
    echo "  --version <version>       Specify the version you want to run"
    echo "  -h, --help  Show this help message"
}

if [ $# -ne 3 ]; then
    print_usage
    exit 1
fi

USE_DEV=false
USE_CANARY=false
VERSION=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --canary)
            USE_CANARY=true
            shift
            ;;
        --dev)
            USE_DEV=true
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        --version)
            if [[ -z $2 ]]; then
                echo "Error: --version requires a value"
                print_usage
                exit 1
            fi
            VERSION="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Set ENVIRONMENT based on flags
if [[ "$USE_CANARY" == true ]]; then
    ENVIRONMENT="canary"
elif [[ "$USE_DEV" == true ]]; then
    ENVIRONMENT="dev"
else
    echo "Error: You must specify either --canary or --dev."
    print_usage
    exit 1
fi

echo "Running with environment $ENVIRONMENT and version $VERSION"

set -xo pipefail

SUFFIX_VALUE=${1:-"$RANDOM"}

python scripts/distribution/generate_templates.py --test-id $SUFFIX_VALUE --output-dir .

# Create a virtualenv to fresh install zipline-ai
VENV_DIR="tmp_chronon"
rm -rf $VENV_DIR
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Download the wheel
WHEEL_FILE="zipline_ai-$VERSION-py3-none-any.whl"
gcloud storage cp gs://zipline-artifacts-$ENVIRONMENT/release/$VERSION/wheels/$WHEEL_FILE .

# Install the wheel (force)
pip uninstall zipline-ai
pip install --force-reinstall $WHEEL_FILE


# function to check dataproc job id state
function check_dataproc_job_state() {
  JOB_ID=$1
  if [ -z "$JOB_ID" ]; then
        echo "No job id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  JOB_STATE=$(gcloud dataproc jobs describe $JOB_ID --region=us-central1 --format=flattened | grep "status.state:" | awk '{print $NF}')
  echo $JOB_STATE
#  TODO: this doesn't actually fail. need to fix.
  if [ -z "$JOB_STATE" ] || [ "$JOB_STATE" == "ERROR" ]; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------JOB FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi
}

function fail_if_bash_failed() {
  if [ $? -ne 0 ]; then
    echo -e "${RED} <<<<<<<<<<<<<<<<-----------------FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
    exit 1
  fi
}

START_DS="2023-11-01"
END_DS="2023-11-30"

CHRONON_ROOT=`pwd`/python/test/canary
export PYTHONPATH="$CHRONON_ROOT" ARTIFACT_PREFIX="gs://zipline-artifacts-$ENVIRONMENT" CUSTOMER_ID=$ENVIRONMENT

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
zipline compile --chronon-root=$CHRONON_ROOT

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline run --repo=$CHRONON_ROOT  --version $VERSION --mode backfill --conf compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_test__0 --start-ds $START_DS --end-ds $END_DS
else
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_dev__0 --start-ds $START_DS --end-ds $END_DS
fi

fail_if_bash_failed $?

echo -e "${GREEN}<<<<<.....................................BACKFILL-JOIN.....................................>>>>>\033[0m"
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline run --repo=$CHRONON_ROOT  --version $VERSION --mode backfill --conf compiled/joins/gcp/training_set_${SUFFIX_VALUE}.v1_test__0 --start-ds $START_DS --end-ds $END_DS
  zipline run --repo=$CHRONON_ROOT  --version $VERSION --mode backfill --conf compiled/joins/gcp/training_set_${SUFFIX_VALUE}.v1_dev_notds__0 --start-ds $START_DS --end-ds $END_DS

else
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/joins/gcp/training_set_${SUFFIX_VALUE}.v1_dev__0 --start-ds $START_DS --end-ds $END_DS
  zipline run --repo=$CHRONON_ROOT  --version $VERSION --mode backfill --conf compiled/joins/gcp/training_set_${SUFFIX_VALUE}.v1_dev_notds__0 --start-ds $START_DS --end-ds $END_DS
fi
fail_if_bash_failed $?


echo -e "${GREEN}<<<<<.....................................CHECK-PARTITIONS.....................................>>>>>\033[0m"
EXPECTED_PARTITION=$END_DS
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline run --repo=$CHRONON_ROOT  --version $VERSION --mode metastore check-partitions --partition-names=data.gcp_purchases_${SUFFIX_VALUE}_v1_test__0/ds=$EXPECTED_PARTITION --conf compiled/teams_metadata/gcp/gcp_team_metadata
else
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode metastore check-partitions --partition-names=data.gcp_purchases_${SUFFIX_VALUE}_v1_dev__0/ds=$EXPECTED_PARTITION --conf compiled/teams_metadata/gcp/gcp_team_metadata
fi
fail_if_bash_failed $?

echo -e "${GREEN}<<<<<.....................................GROUP-BY-UPLOAD.....................................>>>>>\033[0m"
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode upload --conf compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_test__0 --ds $END_DS
else
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode upload --conf compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_dev__0 --ds $END_DS
fi
fail_if_bash_failed

# Need to wait for upload to finish
echo -e "${GREEN}<<<<<.....................................UPLOAD-TO-KV.....................................>>>>>\033[0m"
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode upload-to-kv --conf compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_test__0 --ds=$END_DS
else
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode upload-to-kv --conf compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_dev__0 --ds=$END_DS
fi
fail_if_bash_failed

echo -e "${GREEN}<<<<< .....................................METADATA-UPLOAD.....................................>>>>>\033[0m"
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode metadata-upload --conf compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_test__0
else
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode metadata-upload --conf compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_dev__0
fi
fail_if_bash_failed

echo -e "${GREEN}<<<<< .....................................STAGING-QUERY.....................................>>>>>\033[0m"
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/staging_queries/gcp/exports_${SUFFIX_VALUE}.checkouts_test__0 --start-ds $START_DS --end-ds $END_DS
else
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/staging_queries/gcp/exports_${SUFFIX_VALUE}.checkouts_dev__0 --start-ds $START_DS --end-ds $END_DS
fi
fail_if_bash_failed

# Need to wait for upload-to-kv to finish
echo -e "${GREEN}<<<<<.....................................FETCH.....................................>>>>>\033[0m"
touch tmp_fetch.out
if [[ "$ENVIRONMENT" == "canary" ]]; then
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode fetch --conf=compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_test__0 -k '{"user_id":"5"}' --name gcp.purchases_${SUFFIX_VALUE}.v1_test__0 2>&1 | tee tmp_fetch.out
else
  zipline run --repo=$CHRONON_ROOT --version $VERSION --mode fetch --conf=compiled/group_bys/gcp/purchases_${SUFFIX_VALUE}.v1_dev__0  -k '{"user_id":"5"}' --name gcp.purchases_${SUFFIX_VALUE}.v1_dev__0 2>&1 | tee tmp_fetch.out
fi
cat tmp_fetch.out | grep purchase_price_average_7d
# check if exit code of previous is 0
if [ $? -ne 0 ]; then
  echo "Failed to find purchase_price_average_7d"
  exit 1
fi

echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"

# Clean up
rm `pwd`/python/test/canary/group_bys/gcp/purchases_${SUFFIX_VALUE}.py
rm `pwd`/python/test/canary/joins/gcp/training_set_${SUFFIX_VALUE}.py
rm `pwd`/python/test/canary/staging_queries/gcp/exports_${SUFFIX_VALUE}.py

## Delete gcp tables to cleanup
if [[ "$ENVIRONMENT" == "canary" ]]; then
  bq rm -f -t canary-443022:data.gcp_purchases_${SUFFIX_VALUE}_v1_test__0
  bq rm -f -t canary-443022:data.gcp_purchases_${SUFFIX_VALUE}_v1_test_upload__0
  bq rm -f -t canary-443022:data.gcp_training_set_${SUFFIX_VALUE}_v1_test__0
  bq rm -f -t canary-443022:data.gcp_purchases_${SUFFIX_VALUE}_v1_test_notds__0
  bq rm -f -t canary-443022:data.gcp_training_set_${SUFFIX_VALUE}_v1_test_notds__0
  bq rm -f -t canary-443022:data.gcp_exports_${SUFFIX_VALUE}_checkouts_test__0

else
  bq rm -f -t canary-443022:data.gcp_purchases_${SUFFIX_VALUE}_v1_dev__0
  bq rm -f -t canary-443022:data.gcp_purchases_${SUFFIX_VALUE}_v1_dev_upload__0
  bq rm -f -t canary-443022:data.gcp_training_set_${SUFFIX_VALUE}_v1_dev__0
  bq rm -f -t canary-443022:data.gcp_purchases_${SUFFIX_VALUE}_v1_dev_notds__0
  bq rm -f -t canary-443022:data.gcp_training_set_${SUFFIX_VALUE}_v1_dev_notds__0
  bq rm -f -t canary-443022:data.gcp_exports_${SUFFIX_VALUE}_checkouts_dev__0
fi