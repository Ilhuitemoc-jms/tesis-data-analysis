# scripts/load_parquet_to_mongo.py
"""
Script optimizado para cargar 166M registros desde archivos Parquet a MongoDB
Adaptado para correr dentro del contenedor PySpark/JupyterLab
"""
import os
import sys
import time
import pandas as pd
import pymongo
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError

# Configuración de MongoDB (conecta al servicio mongodb del docker-compose)
MONGO_URI = "mongodb://admin:votaciones2025@mongodb:27017/"
DATABASE_NAME = "votaciones_usa"
COLLECTION_NAME = "registros_votacion"

# Parámetros de optimización
BATCH_SIZE = 35000  # Optimizado para 64GB RAM (no tan agresivo como 30000) 10000
MAX_POOL_SIZE = 250  # Aumentado para mejor concurrencia 100

def get_mongo_connection():
    """
    Establece conexión optimizada con MongoDB
    Usa el nombre del servicio 'mongodb' definido en docker-compose
    """
    """ client = MongoClient(
        MONGO_URI,
        maxPoolSize=MAX_POOL_SIZE,
        minPoolSize=10,
        maxIdleTimeMS=45000,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=None,
        retryWrites=True,
        w=1
    ) """
    # En la función get_mongo_connection()
    client = MongoClient(
        MONGO_URI,
        maxPoolSize=200,  # Aumentado de 100
        minPoolSize=20,   # Aumentado de 10
        maxIdleTimeMS=45000,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=None,
        retryWrites=True,
        w=1  # SEGURO: Confirma escritura en nodo primario
    )
    
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    return client, collection

def create_indexes(collection):
    """
    Crea índices después de la carga masiva
    Más eficiente que crear índices antes de insertar datos
    """
    print("\nCreando índices en la colección...")
    
    # Índice por estado
    collection.create_index([("state", pymongo.ASCENDING)])
    
    # Índice por condado
    collection.create_index([("county", pymongo.ASCENDING)])
    
    # Índice compuesto estado-condado
    collection.create_index([
        ("state", pymongo.ASCENDING),
        ("county", pymongo.ASCENDING)
    ])
    
    print("Índices creados exitosamente")

def get_parquet_files(path):
    """
    Obtiene lista de archivos parquet en estructura anidada
    Recorre recursivamente todos los subdirectorios
    """
    parquet_files = []
    
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                parquet_files.append((file_path, root, file))
    
    return parquet_files

def process_parquet_file(file_info):
    """
    Lee archivo parquet y extrae metadatos de la ruta
    Retorna documentos listos para insertar en MongoDB
    """
    file_path, root, file_name = file_info
    
    try:
        # Lee el archivo parquet
        df = pd.read_parquet(file_path, engine='pyarrow')
        
        # Extrae estado y condado de la estructura de directorios
        path_parts = root.split(os.sep)
        state = None
        county = None
        
        for part in path_parts:
            if part.startswith('state='):
                state = part.replace('state=', '')
            elif part.startswith('county_name='):
                county = part.replace('county_name=', '').replace('%20', ' ')
        
        # Agrega columnas de metadatos
        df['state'] = state
        df['county'] = county
        
        # Convierte a lista de diccionarios (documentos MongoDB)
        documents = df.to_dict('records')
        
        return documents, len(documents), None
        
    except Exception as e:
        return None, 0, str(e)

def bulk_insert_documents(collection, documents):
    """
    Inserta documentos usando bulk write optimizado
    Maneja errores parciales automáticamente
    """
    try:
        # Prepara operaciones de inserción
        requests = [InsertOne(doc) for doc in documents]
        
        # Ejecuta bulk write sin ordenar (más rápido)
        result = collection.bulk_write(requests, ordered=False)
        
        return result.inserted_count, None
        
    except BulkWriteError as bwe:
        # Maneja errores parciales
        inserted = bwe.details.get('nInserted', 0)
        errors = len(bwe.details.get('writeErrors', []))
        return inserted, f"Insertados: {inserted}, Errores: {errors}"
        
    except Exception as e:
        return 0, str(e)

def load_parquet_to_mongo(data_path):
    """
    Función principal que coordina la carga completa
    Optimizada para 166M registros
    """
    print(f"Iniciando carga de datos desde: {data_path}")
    print(f"Tamaño de lote: {BATCH_SIZE:,} documentos")
    
    # Conecta a MongoDB
    client, collection = get_mongo_connection()
    
    # Obtiene lista de archivos
    print("\nBuscando archivos parquet...")
    parquet_files = get_parquet_files(data_path)
    total_files = len(parquet_files)
    print(f"Encontrados {total_files} archivos parquet")
    
    if total_files == 0:
        print(f"No se encontraron archivos .parquet en {data_path}")
        return
    
    # Variables para estadísticas
    total_documents = 0
    total_inserted = 0
    files_processed = 0
    errors = []
    start_time = time.time()
    
    # Buffer para acumular documentos
    document_buffer = []
    
    # Procesa cada archivo
    for idx, file_info in enumerate(parquet_files, 1):
        file_path = file_info[0]
        
        print(f"\nProcesando [{idx}/{total_files}]: {os.path.basename(file_path)}")
        
        # Lee y procesa el archivo
        documents, doc_count, error = process_parquet_file(file_info)
        
        if error:
            print(f"Error: {error}")
            errors.append((file_path, error))
            continue
        
        if documents:
            # Agrega al buffer
            document_buffer.extend(documents)
            total_documents += doc_count
            files_processed += 1
            
            print(f"Leídos {doc_count:,} registros")
            
            # Inserta cuando alcanza el tamaño de lote
            while len(document_buffer) >= BATCH_SIZE:
                batch = document_buffer[:BATCH_SIZE]
                document_buffer = document_buffer[BATCH_SIZE:]
                
                # Ejecuta bulk insert
                inserted, error = bulk_insert_documents(collection, batch)
                total_inserted += inserted
                
                if error:
                    print(f"{error}")
                
                # Muestra progreso
                elapsed = time.time() - start_time
                rate = total_inserted / elapsed if elapsed > 0 else 0
                print(f"Insertados: {total_inserted:,} ({rate:,.0f} docs/seg)")
    
    # Inserta documentos restantes
    if document_buffer:
        print(f"\nInsertando últimos {len(document_buffer):,} documentos...")
        inserted, error = bulk_insert_documents(collection, document_buffer)
        total_inserted += inserted
        if error:
            print(f"{error}")
    
    # Calcula estadísticas finales
    elapsed_time = time.time() - start_time
    avg_rate = total_inserted / elapsed_time if elapsed_time > 0 else 0
    
    # Muestra resumen
    print("\n" + "="*60)
    print("RESUMEN DE CARGA")
    print("="*60)
    print(f"Archivos procesados: {files_processed}/{total_files}")
    print(f"Total documentos leídos: {total_documents:,}")
    print(f"Total documentos insertados: {total_inserted:,}")
    print(f"Tiempo total: {elapsed_time/60:.2f} minutos")
    print(f"Velocidad promedio: {avg_rate:,.0f} documentos/segundo")
    
    if errors:
        print(f"\n Errores encontrados: {len(errors)}")
        for file_path, error in errors[:5]:
            print(f"  - {os.path.basename(file_path)}: {error}")
    
    # Crea índices
    print("\n" + "="*60)
    create_indexes(collection)
    
    # Cierra conexión
    client.close()
    print("\n Proceso completado exitosamente")

if __name__ == "__main__":
    # Ruta a los datos (montada desde docker-compose)
    data_path = "/home/jovyan/data/voting_data"
    
    if not os.path.exists(data_path):
        print(f"Error: La ruta {data_path} no existe")
        print(f"Verifica que los archivos parquet estén en ./data/voting_data/")
        sys.exit(1)
    
    # Ejecuta la carga
    load_parquet_to_mongo(data_path)
