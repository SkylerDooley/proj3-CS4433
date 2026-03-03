FROM bitnami/spark:3.5

USER root

# Install Python dependencies
RUN pip install pyspark faker --break-system-packages 2>/dev/null || \
    pip install pyspark faker

WORKDIR /app
COPY . /app

USER 1001