FROM scratch

ARG VERSION="dev"

# CI prepares build_output/engine/ with cloud-specific JARs:
#   GCP:   cloud_gcp_lib_deploy.jar, flink_assembly_deploy.jar, service_assembly_deploy.jar, connectors_pubsub_deploy.jar
#   AWS:   cloud_aws_lib_deploy.jar, flink_assembly_deploy.jar, service_assembly_deploy.jar, connectors_kinesis_deploy.jar
#   Azure: cloud_azure_lib_deploy.jar, flink_assembly_deploy.jar, service_assembly_deploy.jar
COPY build_output/engine/ /jars/
COPY build_output/engine_manifest.json /manifest.json
# Copy initialization scripts from Python package
COPY python/src/ai/chronon/repo/scripts/ /scripts/
