#!/bin/bash

# default option is to not create a new cluster for testing but use existing cluster
create_cluster=false

# otherwise use the canary cluster id
CANARY_CLUSTER_ID="j-13BASWFP15TLR"

GREEN='\033[0;32m'
RED='\033[0;31m'

function print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --canary | --dev          Must specify the environment (canary or dev)"
    echo "  --version <version>       Must specify the version you want to run"
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
# Delete glue tables
# hardcoding the s3 path here because that's where the underlying location of the data is for this glue database `data`
# Faster to just aws s3 rm than to aws glue delete-table-version + delete-partition and then finally delete-table
if [[ "$ENVIRONMENT" == "canary" ]]; then
  aws s3 rm s3://zipline-warehouse-canary/data/aws_purchases_v1_test__0 --recursive
  aws glue delete-table --database-name data --name quickstart_purchases_v1_test__0
else
  aws s3 rm s3://zipline-warehouse-dev/data/aws_purchases_v1_dev__0 --recursive
  aws glue delete-table --database-name data --name quickstart_purchases_v1_dev__0
fi


# Sleep for a bit since aws glue delete-table is asynchronous
sleep 30

GREEN='\033[0;32m'
RED='\033[0;31m'


#git fetch origin davidhan/run_aws
#git checkout davidhan/run_aws


# Create a virtualenv to fresh install zipline-ai
VENV_DIR="tmp_chronon"
rm -rf $VENV_DIR
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Download the wheel
WHEEL_FILE="zipline_ai-$VERSION-py3-none-any.whl"
aws s3 cp s3://zipline-artifacts-$ENVIRONMENT/release/$VERSION/wheels/$WHEEL_FILE .


# Install the wheel (force)
pip uninstall zipline-ai
pip install --force-reinstall $WHEEL_FILE

export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# function to check dataproc job id state
function check_dataproc_job_state() {
  JOB_ID=$1
  if [ -z "$JOB_ID" ]; then
        echo "No job id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  JOB_STATE=$(gcloud dataproc jobs describe $JOB_ID --region=us-central1 --format=flattened | grep "status.state:")
  echo $JOB_STATE
#  TODO: this doesn't actually fail. need to fix.
  if [ -z "$JOB_STATE" ]; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------JOB FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi
}

function check_emr_step_state() {
  CHECK_STEP_ID=$1
  CHECK_CLUSTER_ID=$2
  if [ -z "$CHECK_STEP_ID" ]; then
        echo "No step id available to check. Exiting."
        exit 1
  fi
  if [ -z "$CHECK_CLUSTER_ID" ]; then
        echo "No cluster id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  # check exit code
  if ! aws emr wait step-complete --cluster-id $CHECK_CLUSTER_ID --step-id $CHECK_STEP_ID; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------JOB FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi

  STEP_STATE=$(aws emr describe-step --cluster-id $CHECK_CLUSTER_ID --step-id $CHECK_STEP_ID --query Step.Status.State | tr -d '"')
  if [ "$STEP_STATE" != "COMPLETED" ]; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------JOB FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi
  echo "succeeded"
}

function check_emr_cluster_state() {
  CHECK_CLUSTER_ID=$1
  if [ -z "$CHECK_CLUSTER_ID" ]; then
        echo "No cluster id available to check. Exiting."
        exit 1
  fi
  echo -e "${GREEN} <<<<<<<<<<<<<<<<-----------------JOB STATUS----------------->>>>>>>>>>>>>>>>>\033[0m"
  if ! aws emr wait cluster-running --cluster-id $CHECK_CLUSTER_ID; then
        echo -e "${RED} <<<<<<<<<<<<<<<<-----------------CLUSTER CREATION FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
        exit 1
  fi
  aws emr describe-cluster --cluster-id $CHECK_CLUSTER_ID
}


CHRONON_ROOT=`pwd`/python/test/canary
export PYTHONPATH="${PYTHONPATH}:$CHRONON_ROOT" ARTIFACT_PREFIX="s3://zipline-artifacts-$ENVIRONMENT" CUSTOMER_ID=$ENVIRONMENT


echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"

zipline compile --chronon-root=$CHRONON_ROOT

echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
touch tmp_backfill.out

if [ "$create_cluster" = true ]; then
  echo "Creating a new EMR cluster"
  if [[ "$ENVIRONMENT" == "canary" ]]; then
    zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/group_bys/aws/purchases.v1_test__0 --end-ds 20250220 --create-cluster --cluster-instance-count=2 --cluster-idle-timeout=60 2>&1 | tee tmp_backfill.out
  else
    zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/group_bys/aws/purchases.v1_dev__0 --end-ds 20250220 --create-cluster --cluster-instance-count=2 --cluster-idle-timeout=60 2>&1 | tee tmp_backfill.out
  fi
  EMR_SUBMITTER_ID_CLUSTER_STR="EMR job id"
  CLUSTER_ID=$(cat tmp_backfill.out | grep "$EMR_SUBMITTER_ID_CLUSTER_STR"  | cut -d " " -f4) # expecting the cluster id to be the 4th field
  check_emr_cluster_state $CLUSTER_ID
  # Get the step id
  STEP_ID=$(aws emr list-steps --cluster-id $CLUSTER_ID | jq -r '.Steps[0].Id')
  check_emr_step_state $STEP_ID $CLUSTER_ID
else
  CLUSTER_ID=$CANARY_CLUSTER_ID
  echo "Using existing EMR cluster $CLUSTER_ID"
  if [[ "$ENVIRONMENT" == "canary" ]]; then
    EMR_CLUSTER_ID=$CLUSTER_ID zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/group_bys/aws/purchases.v1_test__0 --end-ds 20250220 2>&1 | tee tmp_backfill.out
  else
    EMR_CLUSTER_ID=$CLUSTER_ID zipline run --repo=$CHRONON_ROOT --version $VERSION --mode backfill --conf compiled/group_bys/aws/purchases.v1_dev__0 --end-ds 20250220 2>&1 | tee tmp_backfill.out
  fi

  EMR_SUBMITER_ID_STEP_STR="EMR step id"
  STEP_ID=$(cat tmp_backfill.out | grep "$EMR_SUBMITER_ID_STEP_STR"  | cut -d " " -f4) # expecting the step id to be the 4th field
  check_emr_step_state $STEP_ID $CLUSTER_ID
fi


echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"
