# pyspark/Dockerfile
# USA SPARK 3.5.3 (última de la serie 3.5, ESTABLE)
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

LABEL maintainer="tesis-votaciones"
LABEL description="JupyterLab con PySpark para análisis de votaciones"

USER root

RUN apt-get update && apt-get install -y \
    vim \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Conector 10.3.0 para Spark 3.5
RUN wget -q https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar \
    -O /usr/local/spark/jars/mongo-spark-connector_2.12-10.3.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.0/mongodb-driver-sync-5.1.0.jar \
    -O /usr/local/spark/jars/mongodb-driver-sync-5.1.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/bson/5.1.0/bson-5.1.0.jar \
    -O /usr/local/spark/jars/bson-5.1.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.0/mongodb-driver-core-5.1.0.jar \
    -O /usr/local/spark/jars/mongodb-driver-core-5.1.0.jar

RUN chmod 644 /usr/local/spark/jars/mongo-spark-connector_2.12-10.3.0.jar \
    /usr/local/spark/jars/mongodb-driver-sync-5.1.0.jar \
    /usr/local/spark/jars/bson-5.1.0.jar \
    /usr/local/spark/jars/mongodb-driver-core-5.1.0.jar

RUN ln -snf /usr/share/zoneinfo/America/Mexico_City /etc/localtime && echo "America/Mexico_City" > /etc/timezone
ENV TZ=America/Mexico_City

USER ${NB_UID}

# Copia el archivo requirements.txt al contenedor
COPY requirements.txt /tmp/requirements.txt

# Instala todas las dependencias desde requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /home/jovyan/notebooks /home/jovyan/scripts /home/jovyan/data
WORKDIR /home/jovyan

EXPOSE 8888 4040 4041
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
