# Spark on Kubernetes Docker Images

This directory contains Dockerfiles for building Spark images to run Chronon on Kubernetes across different cloud providers.

## Overview

These Docker images extend the official Apache Spark images with cloud-specific dependencies and the Chronon application JAR, enabling Spark workloads to run on managed Kubernetes services like Azure AKS, GCP GKE, and AWS EKS.

## Available Images

### Azure (azure.Dockerfile)

Based on `apache/spark:3.5.3-java17`, this image includes:
- **Hadoop Azure libraries** (3.4.2) for Azure Data Lake Storage Gen2 (ADLS) support
- **Azure Cosmos Spark connector** (4.42.0) for Cosmos DB integration
- Cloud-specific JARs placed in `/opt/spark/jars` for automatic classpath inclusion

**Key Dependencies:**
- `hadoop-azure-3.4.2.jar` - Azure storage filesystem support
- `hadoop-common-3.4.2.jar` - Core Hadoop utilities
- `azure-cosmos-spark_3-5_2-12-4.42.0.jar` - Cosmos DB connector

## Building Images

### Azure
```bash
cd docker/spark
docker build -f azure.Dockerfile -t chronon-spark-azure:latest .
```

### Pushing to Registry
```bash
# Tag for your container registry
docker tag chronon-spark-azure:latest <your-registry>/chronon-spark-azure:<version>

# Push to registry
docker push <your-registry>/chronon-spark-azure:<version>
```

## Using the Images

### On Azure Kubernetes Service (AKS)

1. Push the image to Azure Container Registry:
```bash
az acr login --name <your-acr-name>
docker tag chronon-spark-azure:latest <your-acr-name>.azurecr.io/chronon-spark:latest
docker push <your-acr-name>.azurecr.io/chronon-spark:latest
```

2. Configure your Spark application to use the image:
```yaml
spec:
  image: <your-acr-name>.azurecr.io/chronon-spark:latest
  sparkConf:
    "spark.kubernetes.container.image.pullPolicy": "Always"
```

3. Ensure your AKS cluster has appropriate service account permissions to access ADLS and other Azure resources.

## Application JAR

The `out.jar` file contains the compiled Chronon application code. To update it:

1. Build the Chronon project:
```bash
./mill cloud_azure.assembly  # or appropriate module for your cloud
```

2. Copy the assembled JAR:
```bash
cp out/cloud_azure/assembly.dest/out.jar docker/spark/out.jar
```

3. Rebuild the Docker image with the updated JAR.

## Adding New Cloud Providers

To add support for additional cloud providers (GCP, AWS, etc.):

1. Create a new Dockerfile: `<cloud>.Dockerfile`
2. Base it on `apache/spark:3.5.3-java17`
3. Add cloud-specific connector JARs:
   - **GCP**: `gcs-connector`, `bigquery-connector`
   - **AWS**: `hadoop-aws`, `aws-java-sdk-bundle`
4. Follow the same pattern as `azure.Dockerfile`
5. Update this README with build and usage instructions

## Version Alignment

When updating dependencies, ensure version compatibility:
- Spark version must match the base image
- Hadoop version should align with cloud provider SDK requirements
- Scala version (2.12) must match Spark's Scala version
- Keep dependency versions in sync with those in the Chronon build configuration

## Troubleshooting

### ClassNotFoundException for cloud connectors
- Verify JARs are in `/opt/spark/jars` directory
- Check JAR versions match runtime Spark/Scala versions

### Authentication Issues
- Ensure Kubernetes service accounts have appropriate cloud IAM roles
- For Azure: verify managed identity or service principal configuration
- For GCP: check workload identity setup
- For AWS: verify IRSA (IAM Roles for Service Accounts)

### Out of Memory Errors
- Adjust Spark driver/executor memory settings in your Spark configuration
- Consider the overhead of cloud connector libraries when sizing pods

## References

- [Apache Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Azure Cosmos Spark Connector](https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/cosmos/azure-cosmos-spark_3_2-12)
- [Hadoop Azure ADLS Documentation](https://hadoop.apache.org/docs/stable/hadoop-azure/index.html)
