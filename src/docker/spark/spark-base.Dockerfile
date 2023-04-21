FROM cluster-base

# -- Layer: Apache Spark

ARG spark_version=3.3.1
ARG hadoop_version=3

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

# Download and add Cassandra connecting jar
RUN mkdir ${SPARK_HOME}/lib
RUN curl -L -O https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.13.0/java-driver-core-4.13.0.jar
RUN mv java-driver-core-4.13.0.jar ${SPARK_HOME}/lib/

# Set Spark classpath
ENV SPARK_DIST_CLASSPATH ${SPARK_HOME}/jars/*:${SPARK_HOME}/lib/*

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# -- Runtime

WORKDIR ${SPARK_HOME}