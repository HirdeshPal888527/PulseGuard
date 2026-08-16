FROM bitnami/spark:3.5.1

USER root
WORKDIR /app
COPY spark_processor.py .

# Packages (kafka + postgres connectors) are resolved at submit time via
# spark.jars.packages, so no extra pip installs are needed for PySpark itself.
CMD ["spark-submit", "--master", "local[*]", "/app/spark_processor.py"]
