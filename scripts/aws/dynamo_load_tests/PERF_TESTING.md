# DynamoDB Performance Testing on EC2

This directory contains scripts for running DynamoDB performance tests from an EC2 instance in AWS. 

## Prerequisites

1. **AWS CLI** installed and configured:
   ```bash
   aws configure
   # Enter your AWS Access Key ID, Secret Access Key, and default region
   ```

2. **jq** installed (for JSON parsing):
   ```bash
   # macOS
   brew install jq

   # Linux
   sudo apt-get install jq  # Debian/Ubuntu
   sudo yum install jq      # RHEL/CentOS
   ```

3. **rsync** installed (usually comes with macOS/Linux)

## Quick Start

### 1. Create EC2 Instance

This launches and initializes a t3.xlarge instance in us-west-2:

```bash
cd scripts/aws/dynamo_load_tests
./create-ec2-instance.sh
```

**Configuration (optional):**
```bash
# Use a different region
AWS_REGION=us-east-2 ./create-ec2-instance.sh

# Use a larger instance type
INSTANCE_TYPE=t3.2xlarge ./create-ec2-instance.sh

# Use an existing SSH key
AWS_KEY_NAME=my-existing-key ./create-ec2-instance.sh
```

### 2. Sync Repository to EC2

This copies your local code to the EC2 instance:

```bash
./sync-repo-to-ec2.sh
```

**Re-run this whenever you make code changes locally.**

### 3. Run Performance Test

This runs the DynamoDB perf test on the EC2 instance:

```bash
./run-perf-test-on-ec2.sh
```

**Configuration:**
```bash
# Customize test parameters
PERF_DURATION_MINUTES=10 \
PERF_NUM_THREADS=20 \
PERF_SKIP_LOAD=true \
./run-perf-test-on-ec2.sh

# Available environment variables:
# - PERF_DURATION_MINUTES: Duration per time range (default: 5)
# - PERF_NUM_THREADS: Concurrent threads (default: 10)
# - PERF_SKIP_LOAD: Skip data loading phase (default: false)
# - PERF_TILE_SIZE_MS: Tile size in milliseconds (default: 3600000 = 1 hour)
```

### 4. Terminate Instance (When Done)

To save costs, terminate the instance when you're finished:

```bash
./terminate-ec2-instance.sh
```

## Advanced Usage

### SSH into the Instance

```bash
# Get connection info from state file
cat .ec2-instance-state

# SSH in
ssh -i chronon-perf-test-key.pem ec2-user@<INSTANCE_IP>

# Once connected, you can:
cd ~/chronon
./mill cloud_aws.compile
./mill cloud_aws.test
# etc.
```

### Running Tests Manually on EC2

```bash
ssh -i chronon-perf-test-key.pem ec2-user@<INSTANCE_IP>
cd ~/chronon

# Run with custom settings
CHRONON_PERF_TEST_ENABLED=true \
AWS_DEFAULT_REGION=us-west-2 \
PERF_DURATION_MINUTES=15 \
PERF_NUM_THREADS=50 \
./mill cloud_aws.test.testOnly "ai.chronon.integrations.aws.dynamo_load.DynamoDBPerfTestHarness"
```

## Troubleshooting

### "Permission denied (publickey)"
Your SSH key isn't being found. Check:
```bash
ls -la chronon-perf-test-key.pem  # Should exist and be chmod 400
```

### "Instance not found"
Instance may have been terminated. Create a new one:
```bash
rm .ec2-instance-state
./create-ec2-instance.sh
```

### Test fails with "Table not found"
The DynamoDB table doesn't exist yet. Run with data load:
```bash
PERF_SKIP_LOAD=false ./run-perf-test-on-ec2.sh
```

## Files Created

- `chronon-perf-test-key.pem` - SSH private key (do not commit!)
- `.ec2-instance-state` - Instance metadata (do not commit!)
- `perf-test-results-ec2-*.log` - Test results logs
