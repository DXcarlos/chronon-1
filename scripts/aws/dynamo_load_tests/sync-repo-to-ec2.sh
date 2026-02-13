#!/bin/bash

# Script to sync the local repository to the EC2 instance
# Run this after making code changes and before running tests

set -e

STATE_FILE=".ec2-instance-state"

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

log_info "Syncing to instance: $INSTANCE_ID ($INSTANCE_IP)"

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

# Get repository root
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
log_info "Repository root: $REPO_ROOT"

# Sync repository to instance
log_info "Syncing repository to EC2 instance..."
ssh -i "${KEY_NAME}.pem" -o StrictHostKeyChecking=no ec2-user@"$INSTANCE_IP" "mkdir -p ~/chronon"

rsync -avz -e "ssh -i ${KEY_NAME}.pem -o StrictHostKeyChecking=no" \
    --exclude '.git' \
    --exclude 'out' \
    --exclude 'target' \
    --exclude '.bsp' \
    --exclude '.idea' \
    --exclude '*.log' \
    --exclude '.ec2-instance-state' \
    --exclude '*.pem' \
    "$REPO_ROOT/" ec2-user@"$INSTANCE_IP":~/chronon/

log_success "Repository synced successfully"

echo ""
log_info "Next step: Run the perf test with ./run-perf-test-on-ec2.sh"
