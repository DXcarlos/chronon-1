#!/bin/bash
ARTIFACT_PREFIX=$(/usr/share/google/get_metadata_value attributes/artifact_prefix)
# Copy over the java.security settings file to help with TLS setup during BigTable client init
gcloud storage cp "$ARTIFACT_PREFIX/scripts/java.security" /etc/flink/conf/java.security
