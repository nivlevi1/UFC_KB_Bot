# Use a base image
FROM python:3.8-slim-buster

# Install headless Java and procps (for the "ps" command)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless procps && \
    rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Set the working directory
WORKDIR /app

# Install the required Python packages
RUN pip install \
      pandas \
      s3fs \
      requests \
      beautifulsoup4 \
      pyspark \
      python-telegram-bot==13.7 \
      kafka-python
# Copy the project files into the container
COPY . .
