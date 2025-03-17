# Use an official Python image as a base image
FROM python:3.9-slim

# Set environment variables for Spark and Hive
ENV SPARK_HOME=/opt/spark
ENV HIVE_HOME=/opt/hive
ENV PATH="$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH"

# Install system dependencies: Java, wget, curl, and others
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    nano \
    && rm -rf /var/lib/apt/lists/*

# Download and install Apache Spark
RUN wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz  && \
    tar -xzvf spark-3.5.5-bin-hadoop3.tgz  -C /opt && \
    rm spark-3.5.5-bin-hadoop3.tgz 

# Download and install Apache Hive
RUN wget https://downloads.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz  && \
    tar -xzvf apache-hive-4.0.1-bin.tar.gz  -C /opt && \
    rm apache-hive-4.0.1-bin.tar.gz 

# Set up the Hive Metastore configuration
COPY hive-site.xml $HIVE_HOME/conf/

# Set permissions for Hive and Spark
RUN chmod +x $HIVE_HOME/bin/*

# Install PySpark Python package
RUN pip install pyspark

# Set the working directory (VS Code should work from this directory)
WORKDIR /workspace

# Default command to start a simple Python shell (you can replace this with a script later)
CMD ["python"]
