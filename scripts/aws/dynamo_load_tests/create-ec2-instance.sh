#!/bin/bash

# Script to create and initialize an EC2 instance for running DynamoDB perf tests
# This instance can be reused for multiple test runs

set -e

# Configuration
REGION=${AWS_REGION:-us-west-2}
INSTANCE_TYPE=${INSTANCE_TYPE:-t3.xlarge}  # 4 vCPUs, 16GB RAM - good for perf testing
AMI_ID=""  # Will be auto-detected (Amazon Linux 2023)
KEY_NAME=${AWS_KEY_NAME:-chronon-perf-test-key}
SECURITY_GROUP_NAME="chronon-perf-test-sg"
INSTANCE_NAME="chronon-perf-test"
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

# Check prerequisites
log_info "Checking prerequisites..."
if ! command -v aws &> /dev/null; then
    log_error "AWS CLI is not installed. Please install it first: https://aws.amazon.com/cli/"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    log_error "jq is not installed. Please install it first: brew install jq (on macOS) or apt-get install jq (on Linux)"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity --region "$REGION" &> /dev/null; then
    log_error "AWS credentials not configured or invalid. Please run 'aws configure' first."
    exit 1
fi

log_success "Prerequisites check passed"

# Check if instance already exists
if [ -f "$STATE_FILE" ]; then
    EXISTING_INSTANCE_ID=$(jq -r '.instance_id' "$STATE_FILE" 2>/dev/null || echo "")
    if [ -n "$EXISTING_INSTANCE_ID" ]; then
        INSTANCE_STATE=$(aws ec2 describe-instances \
            --region "$REGION" \
            --instance-ids "$EXISTING_INSTANCE_ID" \
            --query 'Reservations[0].Instances[0].State.Name' \
            --output text 2>/dev/null || echo "terminated")

        if [ "$INSTANCE_STATE" = "running" ] || [ "$INSTANCE_STATE" = "pending" ]; then
            log_warning "Instance $EXISTING_INSTANCE_ID already exists and is $INSTANCE_STATE"
            INSTANCE_IP=$(jq -r '.instance_ip' "$STATE_FILE")
            log_info "Instance IP: $INSTANCE_IP"
            log_info "To connect: ssh -i ${KEY_NAME}.pem ec2-user@$INSTANCE_IP"
            exit 0
        fi
    fi
fi

# Get the latest Amazon Linux 2023 AMI
log_info "Finding latest Amazon Linux 2023 AMI in $REGION..."
AMI_ID=$(aws ec2 describe-images \
    --region "$REGION" \
    --owners amazon \
    --filters "Name=name,Values=al2023-ami-2023.*-x86_64" \
              "Name=state,Values=available" \
    --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
    --output text)

if [ -z "$AMI_ID" ]; then
    log_error "Failed to find Amazon Linux 2023 AMI"
    exit 1
fi
log_success "Using AMI: $AMI_ID"

# Check/create security group
log_info "Checking security group..."
SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "")

if [ -z "$SG_ID" ] || [ "$SG_ID" = "None" ]; then
    log_info "Creating security group..."
    VPC_ID=$(aws ec2 describe-vpcs \
        --region "$REGION" \
        --filters "Name=is-default,Values=true" \
        --query 'Vpcs[0].VpcId' \
        --output text)

    SG_ID=$(aws ec2 create-security-group \
        --region "$REGION" \
        --group-name "$SECURITY_GROUP_NAME" \
        --description "Security group for Chronon perf testing" \
        --vpc-id "$VPC_ID" \
        --query 'GroupId' \
        --output text)

    log_success "Created security group: $SG_ID"
else
    log_success "Using existing security group: $SG_ID"
fi

# Ensure SSH access is configured (whether new or existing SG)
log_info "Ensuring SSH access from your IP..."
MY_IP=$(curl -s https://checkip.amazonaws.com)

# Try to add the SSH rule (will fail gracefully if it already exists)
aws ec2 authorize-security-group-ingress \
    --region "$REGION" \
    --group-id "$SG_ID" \
    --protocol tcp \
    --port 22 \
    --cidr "${MY_IP}/32" > /dev/null 2>&1 || log_info "SSH rule already exists or could not be added"

log_success "SSH access configured for IP: $MY_IP"

# Check/create SSH key pair
log_info "Checking SSH key pair..."
if ! aws ec2 describe-key-pairs --region "$REGION" --key-names "$KEY_NAME" &> /dev/null; then
    log_info "Creating SSH key pair..."
    aws ec2 create-key-pair \
        --region "$REGION" \
        --key-name "$KEY_NAME" \
        --query 'KeyMaterial' \
        --output text > "${KEY_NAME}.pem"
    chmod 400 "${KEY_NAME}.pem"
    log_success "Created key pair: ${KEY_NAME}.pem"
else
    log_success "Using existing key pair: $KEY_NAME"
    if [ ! -f "${KEY_NAME}.pem" ]; then
        log_error "Key pair exists in AWS but ${KEY_NAME}.pem not found locally. Please provide the key file or use a different KEY_NAME."
        exit 1
    fi
fi

# Create user data script for instance initialization
log_info "Preparing instance initialization script..."
cat > /tmp/chronon-userdata.sh << 'USERDATA_EOF'
#!/bin/bash
set -ex

# Install dependencies
yum update -y
yum install -y git java-17-amazon-corretto-devel tar gzip wget

# Install Apache Thrift
cd /tmp
wget -q https://dlcdn.apache.org/thrift/0.21.0/thrift-0.21.0.tar.gz
tar -xzf thrift-0.21.0.tar.gz
cd thrift-0.21.0

# Install build dependencies for Thrift
yum install -y gcc gcc-c++ make automake libtool bison flex openssl-devel boost-devel libevent-devel

# Build and install Thrift (minimal build - just the compiler)
./configure --without-python --without-cpp --without-nodejs --without-java
make && make install

# Verify thrift is installed
/usr/local/bin/thrift --version

# Install mill build tool
curl -L https://github.com/com-lihaoyi/mill/releases/download/0.11.7/0.11.7 > /usr/local/bin/mill
chmod +x /usr/local/bin/mill

# Create a flag file when setup is complete
touch /tmp/instance-ready
USERDATA_EOF

# Launch EC2 instance
log_info "Launching EC2 instance ($INSTANCE_TYPE) in $REGION..."
INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --user-data file:///tmp/chronon-userdata.sh \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME}]" \
    --iam-instance-profile Name=chronon-perf-test-role 2>/dev/null \
    --query 'Instances[0].InstanceId' \
    --output text) || {
    # Retry without IAM role if it doesn't exist
    log_warning "IAM role not found, launching without it (you'll need AWS credentials configured)"
    INSTANCE_ID=$(aws ec2 run-instances \
        --region "$REGION" \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --user-data file:///tmp/chronon-userdata.sh \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME}]" \
        --query 'Instances[0].InstanceId' \
        --output text)
}

log_success "Instance launched: $INSTANCE_ID"

# Wait for instance to be running
log_info "Waiting for instance to start..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

# Get instance public IP
INSTANCE_IP=$(aws ec2 describe-instances \
    --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text)

log_success "Instance running at: $INSTANCE_IP"

# Save state
cat > "$STATE_FILE" << EOF
{
  "instance_id": "$INSTANCE_ID",
  "instance_ip": "$INSTANCE_IP",
  "region": "$REGION",
  "key_name": "$KEY_NAME",
  "created_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

log_success "Instance state saved to $STATE_FILE"

# Wait for SSH to be available
log_info "Waiting for SSH to be available..."
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if ssh -i "${KEY_NAME}.pem" -o StrictHostKeyChecking=no -o ConnectTimeout=5 ec2-user@"$INSTANCE_IP" "echo SSH ready" &> /dev/null; then
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo -n "."
    sleep 10
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    log_error "SSH connection timeout"
    exit 1
fi
echo ""
log_success "SSH connection established"

# Wait for user-data script to complete
log_info "Waiting for instance initialization to complete..."
MAX_RETRIES=60
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if ssh -i "${KEY_NAME}.pem" -o StrictHostKeyChecking=no ec2-user@"$INSTANCE_IP" "test -f /tmp/instance-ready" &> /dev/null; then
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo -n "."
    sleep 10
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    log_error "Instance initialization timeout"
    exit 1
fi
echo ""
log_success "Instance initialization complete"

echo ""
echo "==================================================================="
log_success "EC2 Instance Ready!"
echo "==================================================================="
log_info "Instance ID: $INSTANCE_ID"
log_info "Instance IP: $INSTANCE_IP"
log_info "Region: $REGION"
log_info "SSH Command: ssh -i ${KEY_NAME}.pem ec2-user@$INSTANCE_IP"
echo ""
log_info "Next steps:"
log_info "  1. Sync your repo: ./sync-repo-to-ec2.sh"
log_info "  2. Run perf test: ./run-perf-test-on-ec2.sh"
echo "==================================================================="
