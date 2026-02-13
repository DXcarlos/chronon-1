#!/bin/bash

# Script to run the DynamoDB perf test on the EC2 instance
# Retrieves results when complete

set -e

STATE_FILE=".ec2-instance-state"

# Configuration from environment variables
PERF_DURATION_MINUTES=${PERF_DURATION_MINUTES:-5}
PERF_NUM_THREADS=${PERF_NUM_THREADS:-10}
PERF_SKIP_LOAD=${PERF_SKIP_LOAD:-true}
PERF_TILE_SIZE_MS=${PERF_TILE_SIZE_MS:-3600000}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if instance state file exists
if [ ! -f "$STATE_FILE" ]; then
    log_error "No instance state file found. Please run ./create-ec2-instance.sh first."
    exit 1
fi

# Load instance information
if ! command -v jq &> /dev/null; then
    log_error "jq is not installed. Please install it first: brew install jq (on macOS) or apt-get install jq (on Linux)"
    exit 1
fi

INSTANCE_ID=$(jq -r '.instance_id' "$STATE_FILE")
INSTANCE_IP=$(jq -r '.instance_ip' "$STATE_FILE")
REGION=$(jq -r '.region' "$STATE_FILE")
KEY_NAME=$(jq -r '.key_name' "$STATE_FILE")

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "null" ]; then
    log_error "Invalid state file. Please run ./create-ec2-instance.sh first."
    exit 1
fi

log_info "Running perf test on instance: $INSTANCE_ID ($INSTANCE_IP)"

# Check if instance is running
INSTANCE_STATE=$(aws ec2 describe-instances \
    --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].State.Name' \
    --output text 2>/dev/null || echo "not-found")

if [ "$INSTANCE_STATE" != "running" ]; then
    log_error "Instance is not running (state: $INSTANCE_STATE). Please start it first."
    exit 1
fi

# Check SSH connection
log_info "Checking SSH connection..."
if ! ssh -i "${KEY_NAME}.pem" -o StrictHostKeyChecking=no -o ConnectTimeout=5 ec2-user@"$INSTANCE_IP" "echo connected" &> /dev/null; then
    log_error "Cannot connect to instance via SSH"
    exit 1
fi
log_success "SSH connection OK"

# Check if chronon directory exists
if ! ssh -i "${KEY_NAME}.pem" -o StrictHostKeyChecking=no ec2-user@"$INSTANCE_IP" "test -d ~/chronon" &> /dev/null; then
    log_error "Chronon repository not found on instance. Please run ./sync-repo-to-ec2.sh first."
    exit 1
fi

# Run the perf test
echo ""
echo "==================================================================="
log_info "Starting DynamoDB Performance Test"
echo "==================================================================="
log_info "Region: $REGION"
log_info "Duration per range: $PERF_DURATION_MINUTES minutes"
log_info "Concurrent threads: $PERF_NUM_THREADS"
log_info "Skip data load: $PERF_SKIP_LOAD"
log_info "Tile size: $PERF_TILE_SIZE_MS ms"
echo "==================================================================="
echo ""

ssh -i "${KEY_NAME}.pem" -o StrictHostKeyChecking=no ec2-user@"$INSTANCE_IP" << EOF
cd ~/chronon
export CHRONON_PERF_TEST_ENABLED=true
export AWS_DEFAULT_REGION=$REGION
export PERF_DURATION_MINUTES=$PERF_DURATION_MINUTES
export PERF_NUM_THREADS=$PERF_NUM_THREADS
export PERF_SKIP_LOAD=$PERF_SKIP_LOAD
export PERF_TILE_SIZE_MS=$PERF_TILE_SIZE_MS

echo "Starting test..."
./mill cloud_aws.test.testOnly "ai.chronon.integrations.aws.dynamo_load.DynamoDBPerfTestHarness" 2>&1 | tee ~/perf-test-results.log
echo "Test complete!"
EOF

log_success "Perf test completed!"

# Retrieve results
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
RESULTS_FILE="perf-test-results-ec2-${TIMESTAMP}.log"
log_info "Retrieving test results..."
scp -i "${KEY_NAME}.pem" -o StrictHostKeyChecking=no ec2-user@"$INSTANCE_IP":~/perf-test-results.log "./$RESULTS_FILE"

log_success "Results saved to: ./$RESULTS_FILE"

# Extract and display the performance summary
echo ""
echo "==================================================================="
echo "PERFORMANCE RESULTS (from EC2 in $REGION)"
echo "==================================================================="
grep -A 10 "Range.*Requests.*P50" "./$RESULTS_FILE" | tail -11 || log_warning "Could not extract summary table"
echo "==================================================================="
echo ""

log_success "Done! Full results in: ./$RESULTS_FILE"
echo ""
log_info "To run again with different settings:"
log_info "  PERF_DURATION_MINUTES=10 PERF_NUM_THREADS=20 ./run-perf-test-on-ec2.sh"
echo ""
log_info "To clean up the instance:"
log_info "  ./terminate-ec2-instance.sh"
