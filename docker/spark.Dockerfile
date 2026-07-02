FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

ARG ICEBERG_VERSION=1.6.1
ARG SCALA_VERSION=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ENV SPARK_JARS_DIR=/opt/spark/jars

RUN curl -fsSL -o ${SPARK_JARS_DIR}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o ${SPARK_JARS_DIR}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o ${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSL -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

RUN pip install --no-cache-dir requests==2.32.3 boto3==1.35.24

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY spark_jobs /opt/spark_jobs
COPY ingestion /opt/ingestion

WORKDIR /opt/spark
USER spark
