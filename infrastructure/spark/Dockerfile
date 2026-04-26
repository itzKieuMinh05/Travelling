FROM eclipse-temurin:17-jdk

# Install base packages
RUN apt-get update && apt-get install -y \
    curl wget procps net-tools python3 python3-pip dos2unix \
    && rm -rf /var/lib/apt/lists/*

# ===== Versions =====
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV ICEBERG_VERSION=1.9.2
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# ===== Install Spark (download trực tiếp cho chắc) =====
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# ===== Python libs =====
RUN pip3 install --break-system-packages pyiceberg[s3fs,pyarrow] protobuf==5.26.1

# ===== Iceberg =====
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    -O $SPARK_HOME/jars/iceberg.jar

# ===== AWS S3 =====
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar \
    -O $SPARK_HOME/jars/aws.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -O $SPARK_HOME/jars/hadoop-aws.jar

# ===== Kafka connector =====
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    -O $SPARK_HOME/jars/kafka.jar

# ===== User =====
RUN useradd -r -u 185 -g 0 spark && \
    mkdir -p $SPARK_HOME/work-dir && \
    chown -R spark:0 $SPARK_HOME && \
    chmod -R g+w $SPARK_HOME

WORKDIR $SPARK_HOME
USER spark

EXPOSE 7077 8080 8081

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]