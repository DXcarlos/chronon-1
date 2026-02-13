#!/bin/bash

# Script to terminate the EC2 instance created for perf testing

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
    log_error "No instance state file found. Nothing to terminate."
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

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "null" ]; then
    log_error "Invalid state file."
    exit 1
fi

# Check current instance state
INSTANCE_STATE=$(aws ec2 describe-instances \
    --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].State.Name' \
    --output text 2>/dev/null || echo "not-found")

if [ "$INSTANCE_STATE" = "terminated" ] || [ "$INSTANCE_STATE" = "not-found" ]; then
    log_warning "Instance $INSTANCE_ID is already terminated."
    rm -f "$STATE_FILE"
    log_success "Cleaned up state file"
    exit 0
fi

# Confirm termination
echo ""
log_warning "About to terminate instance:"
log_info "  Instance ID: $INSTANCE_ID"
log_info "  Instance IP: $INSTANCE_IP"
log_info "  Region: $REGION"
log_info "  Current State: $INSTANCE_STATE"
echo ""

read -p "Are you sure you want to terminate this instance? (yes/no): " -r
echo

if [ "$REPLY" != "yes" ]; then
    log_info "Termination cancelled"
    exit 0
fi

# Terminate the instance
log_info "Terminating instance $INSTANCE_ID..."
aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION" > /dev/null

log_success "Instance termination initiated"

# Wait for termination
log_info "Waiting for instance to terminate..."
aws ec2 wait instance-terminated --region "$REGION" --instance-ids "$INSTANCE_ID" 2>/dev/null || true

# Clean up state file
rm -f "$STATE_FILE"

log_success "Instance terminated and state file cleaned up"
echo ""
log_info "To create a new instance: ./create-ec2-instance.sh"
