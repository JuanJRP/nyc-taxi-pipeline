FROM python:3.11-bookworm

# Java (requerido por PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/

# Directorio de datos montado como volumen
RUN mkdir -p /app/data/raw /app/data/staging /app/data/processed \
             /app/data/output /app/charts

CMD ["python", "scripts/run_pipeline.py"]
