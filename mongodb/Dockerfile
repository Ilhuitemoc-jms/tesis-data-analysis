# Last stable mongodb image 
FROM mongo:7.0

# Labels for matedata of the container, it doesn't affect the performance
LABEL maintainer="tesis-votaciones"
LABEL description="MongoDB optimizado para 166M registros de votaciones"

# copies settings from the build context to the container, i.e copies mongod.condfig
COPY mongod.conf /etc/mongod.conf

# Crea directorios necesarios
RUN mkdir -p /data/db /var/log/mongodb

# It defines properly keys, chown is a Unix comand to change ownership in order to mongodb write in those directories
RUN chown -R mongodb:mongodb /data/db /var/log/mongodb

# Exposes standar port to Mondo db 
EXPOSE 27017

# Personalice config
CMD ["mongod", "--config", "/etc/mongod.conf"]

# -R        change it to a recursive order