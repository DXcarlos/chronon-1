# Build and Start Script - Quick Guide

The `build_and_start.sh` script is a comprehensive tool that builds the Chronon Fetcher service and starts it with debugging enabled.

## Quick Start

```bash
# Navigate to the docker/fetcher directory
cd docker/fetcher

# Run the script (builds and starts the service)
./build_and_start.sh
```

## What It Does

The script performs these steps automatically:

1. **📦 Builds JARs** - Compiles the required JARs using mill:
   - `cloud_gcp` module
   - `service` module

2. **📁 Prepares build_output** - Copies JARs to the required location for Docker build

3. **🐳 Builds Docker Image** - Creates the `ziplineai/chronon-fetcher:latest` image

4. **🚀 Starts Service** - Launches the service using docker compose with:
   - Main API on port 9000
   - Prometheus metrics (Chronon) on port 8907
   - Prometheus metrics (Vert.x) on port 8908
   - **Java debugger on port 5005** (for remote debugging)
   - Bigtable timeout settings optimized per Google recommendations

## Command Line Options

```bash
./build_and_start.sh [OPTIONS]
```

### Available Options

| Option | Description |
|--------|-------------|
| `--skip-build` | Skip JAR compilation (use existing build_output) |
| `--rebuild` | Clean and rebuild Docker image (no cache) |
| `-d, --detach` | Run containers in detached mode (background) |
| `-h, --help` | Show help message |

### Examples

```bash
# Build and start in detached mode (background)
./build_and_start.sh -d

# Skip JAR build, just rebuild Docker image and start
./build_and_start.sh --skip-build

# Force rebuild Docker image without cache
./build_and_start.sh --rebuild

# Quick restart without rebuilding JARs
./build_and_start.sh --skip-build -d
```

## Configuration

### Environment Variables

Create or edit the `.env` file in `docker/fetcher/`:

```bash
# Copy example to start
cp .env.example .env

# Edit with your values
nano .env
```

Required variables:
- `GCP_PROJECT_ID` - Your GCP project ID
- `GCP_BIGTABLE_INSTANCE_ID` - Your Bigtable instance ID
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to GCP credentials JSON (optional, defaults to `~/.config/gcloud/application_default_credentials.json`)

### Service Ports

| Port | Service | Description |
|------|---------|-------------|
| 9000 | Main API | Chronon Fetcher REST API |
| 8907 | Metrics | Prometheus metrics for Chronon library |
| 8908 | Metrics | Prometheus metrics for Vert.x webserver |
| 5005 | Debug | Java remote debugging port |

## Remote Debugging

The service starts with Java debug agent enabled on port 5005. Connect your IDE:

### IntelliJ IDEA
1. Run → Edit Configurations
2. Add New Configuration → Remote JVM Debug
3. Set Host: `localhost`
4. Set Port: `5005`
5. Click Debug

### VS Code
Add to `.vscode/launch.json`:
```json
{
  "type": "java",
  "name": "Attach to Chronon Fetcher",
  "request": "attach",
  "hostName": "localhost",
  "port": 5005
}
```

## Testing the Service

Once started, test the service:

```bash
# Health check
curl http://localhost:9000/ping

# List available joins
curl http://localhost:9000/v1/joins

# View Chronon metrics
curl http://localhost:8907/metrics

# View Vert.x metrics
curl http://localhost:8908/metrics
```

## Viewing Logs

```bash
# Follow logs (if running detached)
docker compose -f docker/fetcher/docker-compose-debug.yml logs -f

# View recent logs
docker compose -f docker/fetcher/docker-compose-debug.yml logs --tail=100
```

## Stopping the Service

```bash
# Stop the service
docker compose -f docker/fetcher/docker-compose-debug.yml down

# Stop and remove volumes
docker compose -f docker/fetcher/docker-compose-debug.yml down -v
```

## Troubleshooting

### Build fails with "JAR not found"

Make sure you're running from the Chronon root directory, or the script will automatically navigate there.

### Docker build fails

Try rebuilding without cache:
```bash
./build_and_start.sh --rebuild
```

### Service won't start - missing credentials

Ensure your `.env` file has the correct values:
```bash
cat docker/fetcher/.env
```

And verify your GCP credentials file exists:
```bash
ls -la ~/.config/gcloud/application_default_credentials.json
```

### Can't connect to debug port

Make sure port 5005 is not already in use:
```bash
lsof -i :5005
```

### Bigtable timeout errors

The service is pre-configured with increased timeout settings (1s initial, 2s max, 5s total) as recommended by Google. If you still see timeouts, you can adjust these in the docker-compose-debug.yml file:

```yaml
environment:
  - BIGTABLE_INITIAL_RPC_TIMEOUT_DURATION=PT2S  # Increase to 2 seconds
  - BIGTABLE_MAX_RPC_TIMEOUT_DURATION=PT5S      # Increase to 5 seconds
  - BIGTABLE_TOTAL_TIMEOUT_DURATION=PT10S       # Increase to 10 seconds
```

## Advanced Usage

### Custom Docker Compose Configuration

The script generates `docker-compose-debug.yml`. To customize:

1. Run the script once to generate the file
2. Edit `docker/fetcher/docker-compose-debug.yml`
3. Use `--skip-build` to avoid regenerating:
   ```bash
   ./build_and_start.sh --skip-build
   ```

### Build-time Environment Variables

You can pass environment variables during build:

```bash
# Example: Use a different GCP project
GCP_PROJECT_ID=my-other-project ./build_and_start.sh
```

## Files Created

The script creates:
- `docker-compose-debug.yml` - Docker compose configuration with debug settings
- `.env` - Environment variables (if it doesn't exist, copied from `.env.example`)
- Docker image: `ziplineai/chronon-fetcher:latest`

## Performance Notes

### Bigtable Timeout Settings

The service includes optimized Bigtable RPC timeout settings:
- **Initial RPC timeout**: 1 second (up from 200ms default)
- **Max RPC timeout**: 2 seconds
- **Total timeout**: 5 seconds

These settings prevent premature timeout errors while maintaining reasonable response times.

### Memory Settings

The JVM is configured with:
- MaxRAMPercentage=70.0
- MinRAMPercentage=70.0
- InitialRAMPercentage=70.0

This means the JVM will use 70% of available container memory.

## Integration with Debugging Workflow

This script is ideal for the enhanced statistics debugging workflow:

1. **Start service with debugging**:
   ```bash
   ./build_and_start.sh -d
   ```

2. **Attach debugger** to port 5005

3. **Trigger the stats endpoint** to see detailed denormalization logs:
   ```bash
   curl "http://localhost:9000/v1/stats/your_table?startTime=...&endTime=..."
   ```

4. **View logs** to see which columns are failing:
   ```bash
   docker compose -f docker-compose-debug.yml logs -f | grep "denormalization"
   ```

The enhanced debug logging in `JavaStatsService` will show exactly which columns succeed or fail during denormalization.
