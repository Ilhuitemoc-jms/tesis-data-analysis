# pyspark/Dockerfile
# Imagen oficial con JupyterLab + PySpark pre-instalado
FROM quay.io/jupyter/pyspark-notebook:latest

# Información del contenedor
LABEL maintainer="tesis-votaciones"
LABEL description="JupyterLab con PySpark para análisis de votaciones"

# Cambia a root para instalar paquetes del sistema y descargar jars
USER root

# Instala utilidades básicas
RUN apt-get update && apt-get install -y \
    vim \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Descarga conector MongoDB-Spark (como root para tener permisos)
RUN wget -q https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.0/mongo-spark-connector_2.12-10.2.0.jar \
    -O /usr/local/spark/jars/mongo-spark-connector_2.12-10.2.0.jar

# Asegura permisos correctos en el directorio de jars
RUN chmod 644 /usr/local/spark/jars/mongo-spark-connector_2.12-10.2.0.jar

# Regresa a usuario jovyan
USER ${NB_UID}

# Instala paquetes Python adicionales
RUN pip install --no-cache-dir \
    pymongo==4.6.1 \
    fastparquet==2023.10.1

# Descarga TODOS los JARs necesarios para MongoDB-Spark
RUN wget -q https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.0/mongo-spark-connector_2.12-10.2.0.jar \
    -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar \
    -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar \
    -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar \
    -P /usr/local/spark/jars/

# Crea directorios de trabajo
RUN mkdir -p /home/jovyan/notebooks /home/jovyan/scripts /home/jovyan/data

# Establece directorio de trabajo
WORKDIR /home/jovyan

# Expone puertos necesarios
EXPOSE 8888 4040 4041

# Inicia JupyterLab sin token (para desarrollo local)
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
