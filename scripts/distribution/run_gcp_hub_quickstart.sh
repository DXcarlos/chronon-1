#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'

function print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --canary | --dev          Specify the environment (canary or dev)"
    echo "  --version <version>       Specify the version you want to run"
    echo "  -h, --help  Show this help message"
    echo "  --hub-url <url>           Specify the hub URL"
}

if [ $# -ne 5 ]; then
    print_usage
    exit 1
fi

USE_DEV=false
USE_CANARY=false
HUB_URL=""
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
        --hub-url)
            if [[ -z $2 ]]; then
                echo "Error: --hub requires a value"
                print_usage
                exit 1
            fi
            HUB_URL="$2"
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

set -xo pipefail

# Delete output table
# TODO: update this new conf
bq rm -f -t canary-443022:data.gcp_training_set_v1_hub__0
bq rm -f -t canary-443022.data.gcp_sample_staging_query_v1_hub__0

# Every one gets their own name spaced TABLE_PARTITIONS bigtable
export TABLE_PARTITIONS_DATASET="TABLE_PARTITIONS_CI"
# TODO: we need to setup a separate hub for ci testing since we're going to be dropping and recreating bigtable tables
cbt -project canary-443022 -instance zipline-canary-instance deletetable $TABLE_PARTITIONS_DATASET &&
cbt -project canary-443022 -instance zipline-canary-instance createtable $TABLE_PARTITIONS_DATASET &&
cbt -project canary-443022 -instance zipline-canary-instance createfamily $TABLE_PARTITIONS_DATASET cf

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

function extract_workflow_id() {
  "$@" | grep "workflowId" | sed "s/.*'workflowId': '\([^']*\)'.*/\1/"
}

function check_workflow_id_not_empty() {
  if [ -n "$1" ]; then
      echo "$1"
  else
      echo "No workflowId found. Exiting..." >&2
      exit 1
  fi
}

function fail_if_bash_failed() {
  if [ $? -ne 0 ]; then
    echo -e "${RED} <<<<<<<<<<<<<<<<-----------------FAILED!----------------->>>>>>>>>>>>>>>>>\033[0m"
    exit 1
  fi
}

check_workflow_status() {
#      UNKNOWN = 0,
#      SUBMITTED = 1,
#      RUNNING = 2,
#      SUCCEEDED = 3,
#      FAILED = 4,
#      WAITING = 5

    local hub_url="$1"
    local max_attempts=10
    local attempt=1

    if [ -z "$hub_url" ]; then
        echo "Set the hub url"
        return 1
    fi

    while [ $attempt -le $max_attempts ]; do
        echo "Attempt $attempt/$max_attempts: Checking workflow status..."

        # Make the curl request and extract status
        if [[ -z "$GCP_ID_TOKEN" ]]; then
          response=$(curl -s -X GET "$hub_url")
        else
          response=$(curl -s -X GET "$hub_url" -H "Authorization: Bearer $GCP_ID_TOKEN")
        fi
        echo "Workflow response: $response"
        status=$(echo "$response" | grep -o '"status":[0-9]*' | head -n1 | cut -d':' -f2 | tr -d ' \n\r')

        # Map status number to string
        case "$status" in
            0) status_name="UNKNOWN" ;;
            1) status_name="SUBMITTED" ;;
            2) status_name="RUNNING" ;;
            3) status_name="SUCCEEDED" ;;
            4) status_name="FAILED" ;;
            5) status_name="WAITING" ;;
            *) status_name="INVALID" ;;
        esac

        echo "Current status: $status ($status_name)"

        case $status in
            3)
                echo "Success! Workflow status is 3"
                return 0
                ;;
            0|4)
                echo "Error: Workflow status is $status, exiting with error"
                exit 1
                ;;
            *)
                echo "Status is $status, waiting 30 seconds before retry..."
                sleep 30
                ;;
        esac

        attempt=$((attempt + 1))
    done

    echo "Maximum attempts ($max_attempts) reached. Workflow did not reach status 3."
    return 1
}

CHRONON_ROOT=`pwd`/api/python/test/canary
export PYTHONPATH="$CHRONON_ROOT" ARTIFACT_PREFIX="gs://zipline-artifacts-$ENVIRONMENT" CUSTOMER_ID=$ENVIRONMENT

echo -e "${GREEN}<<<<<.....................................COMPILE.....................................>>>>>\033[0m"
zipline compile --chronon-root=$CHRONON_ROOT

touch tmp_backfill.out
echo -e "${GREEN}<<<<<.....................................BACKFILL.....................................>>>>>\033[0m"
zipline hub backfill --repo=$CHRONON_ROOT --conf compiled/staging_queries/gcp/sample_staging_query.v1_hub__0 --start-ds 2023-12-01 --end-ds 2023-12-01 | tee tmp_backfill.out


fail_if_bash_failed $?
WORKFLOW_ID=$(extract_workflow_id cat tmp_backfill.out)
check_workflow_id_not_empty $WORKFLOW_ID

echo -e "${GREEN}<<<<<.....................................CHECK-WORKFLOW_ID.....................................>>>>>\033[0m"
check_workflow_status "$HUB_URL/workflow/$WORKFLOW_ID"
fail_if_bash_failed $?

echo -e "${GREEN}<<<<<.....................................SUCCEEDED!!!.....................................>>>>>\033[0m"
