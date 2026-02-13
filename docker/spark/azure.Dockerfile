FROM apache/spark:3.5.3-java17

USER root

ENV SPARK_JARS_DIR=/opt/spark/jars
ENV AZURE_LOG_LEVEL=verbose

# It is highly recommended to keep these versions aligned to avoid runtime conflicts
# - Hadoop: 3.4.2 (newer version for better Azure support, independent of hadoop-client in app jar)
# - Azure SDK versions match those resolved in cloud_azure assembly jar
RUN  curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.4.2/hadoop-azure-3.4.2.jar -o ${SPARK_JARS_DIR}/hadoop-azure-3.4.2.jar \
  && curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.2/hadoop-common-3.4.2.jar -o ${SPARK_JARS_DIR}/hadoop-common-3.4.2.jar \
  && curl -fL https://repo1.maven.org/maven2/com/azure/cosmos/spark/azure-cosmos-spark_3-5_2-12/4.42.0/azure-cosmos-spark_3-5_2-12-4.42.0.jar -o ${SPARK_JARS_DIR}/azure-cosmos-spark_3-5_2-12-4.42.0.jar 

RUN chown -R spark:spark ${SPARK_JARS_DIR}

USER spark
