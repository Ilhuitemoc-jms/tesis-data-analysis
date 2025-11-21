# scripts/generate_timestamps_ultra_optimized.py
"""
Script ultra-optimizado para generar timestamps
Solo 3 variables: loaded_at, voting_hour, blockchain_delay_seconds
Maxima velocidad para Xeon E-2146G, 64GB RAM, SSD NVMe Gen4
"""

import random
import time
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError


# Configuracion de MongoDB
MONGO_URI = "mongodb://admin:votaciones2025@mongodb:27017/"
DATABASE_NAME = "votaciones_usa"
COLLECTION_NAME = "registros_votacion"

# Parametros de optimizacion
BATCH_SIZE = 30000
MAX_POOL_SIZE = 220

# Configuracion de votacion
ELECTION_DATE = "2024-11-05"
VOTING_HOURS = 12
END_CONCENTRATION = 30


def create_hourly_distribution(total_hours, end_weight_percentage=30):
    """
    Crea distribucion horaria de votacion
    """
    if total_hours <= 12:
        base_weights = {
            0: 0.02, 1: 0.08, 2: 0.12, 3: 0.10, 4: 0.08, 5: 0.06,
            6: 0.04, 7: 0.03, 8: 0.05, 9: 0.10, 10: 0.15, 11: 0.17
        }
    elif total_hours <= 16:
        base_weights = {
            0: 0.02, 1: 0.06, 2: 0.10, 3: 0.08, 4: 0.06, 5: 0.05,
            6: 0.04, 7: 0.04, 8: 0.05, 9: 0.06, 10: 0.07, 11: 0.08,
            12: 0.10, 13: 0.08, 14: 0.10, 15: 0.11
        }
    else:
        base_weights = {
            0: 0.005, 1: 0.003, 2: 0.002, 3: 0.001, 4: 0.001, 5: 0.002,
            6: 0.008, 7: 0.025, 8: 0.045, 9: 0.055, 10: 0.050, 11: 0.045,
            12: 0.035, 13: 0.030, 14: 0.040, 15: 0.050, 16: 0.060, 17: 0.070,
            18: 0.085, 19: 0.070, 20: 0.045, 21: 0.025, 22: 0.015, 23: 0.008
        }
    
    hourly_weights = {}
    for hour in range(total_hours):
        if hour < len(base_weights):
            hourly_weights[hour] = base_weights[hour]
        else:
            hourly_weights[hour] = base_weights[hour % len(base_weights)]
    
    total_base_weight = sum(hourly_weights.values())
    remaining_weight = 1.0 - (end_weight_percentage / 100.0)
    
    for hour in hourly_weights:
        hourly_weights[hour] = (hourly_weights[hour] / total_base_weight) * remaining_weight
    
    final_hours = min(2, total_hours)
    end_weight_per_hour = (end_weight_percentage / 100.0) / final_hours
    
    for i in range(final_hours):
        final_hour = total_hours - 1 - i
        hourly_weights[final_hour] += end_weight_per_hour
    
    return hourly_weights


def create_weighted_hour_choices(hourly_weights):
    """
    Crea lista ponderada para seleccion aleatoria de hora
    """
    hours = []
    for hour, weight in hourly_weights.items():
        count = int(weight * 10000)
        hours.extend([hour] * count)
    return hours


def generate_voting_timeframe(election_date, voting_hours):
    """
    Genera ventana de tiempo de votacion
    """
    voting_start = election_date.replace(hour=6, minute=0, second=0, microsecond=0)
    voting_end = voting_start + timedelta(hours=voting_hours)
    return voting_start, voting_end


def generate_ultra_optimized_timestamp(voting_start, weighted_hours):
    """
    Genera timestamp ultra-optimizado
    Calcula loaded_at en UTC directamente y extrae voting_hour del mismo
    
    Args:
        voting_start (datetime): Inicio del periodo de votacion (con timezone)
        weighted_hours (list): Lista de horas ponderadas
        
    Returns:
        tuple: (datetime loaded_at UTC, str voting_hour HH:MM:SS, float blockchain_delay)
    """
    # Selecciona hora y genera tiempo completo
    target_hour = random.choice(weighted_hours)
    
    # Crea timestamp con componentes aleatorios
    target_time = voting_start.replace(
        hour=target_hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999)
    )
    
    # Convierte a UTC (sin timezone info para MongoDB)
    loaded_at = target_time.astimezone(pytz.UTC).replace(tzinfo=None)
    
    # Extrae voting_hour directamente de loaded_at
    voting_hour_str = loaded_at.strftime("%H:%M:%S")
    
    # Genera blockchain delay como dato representativo
    blockchain_delay = random.uniform(1, 30)
    
    return loaded_at, voting_hour_str, blockchain_delay


def update_voting_timestamps(election_date_str, voting_hours=12, 
                           end_concentration_pct=30):
    """
    Funcion principal ultra-optimizada
    Solo crea 3 campos: loaded_at, voting_hour, blockchain_delay_seconds
    """
    print("="*70)
    print("GENERACION ULTRA-OPTIMIZADA DE TIMESTAMPS")
    print("="*70)
    print(f"Configuracion:")
    print(f"  Fecha de eleccion: {election_date_str}")
    print(f"  Ventana de votacion: {voting_hours} horas")
    print(f"  Concentracion final: {end_concentration_pct}%")
    print(f"  Campos a crear: loaded_at, voting_hour, blockchain_delay_seconds")
    
    # Parsea fecha de eleccion
    try:
        election_date = datetime.strptime(election_date_str, "%Y-%m-%d")
        est = pytz.timezone('US/Eastern')
        election_date = est.localize(election_date)
    except ValueError:
        raise ValueError("Election date must be in YYYY-MM-DD format")
    
    # Conecta a MongoDB
    print("\nConectando a MongoDB...")
    client = MongoClient(
        MONGO_URI,
        maxPoolSize=MAX_POOL_SIZE,
        minPoolSize=20,
        maxIdleTimeMS=45000,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=None,
        retryWrites=True,
        w=1 
    )

    """ 
    MONGO_URI,
    maxPoolSize=MAX_POOL_SIZE,
    minPoolSize=20,
    maxIdleTimeMS=45000,
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=10000,
    socketTimeoutMS=None,
    retryWrites=True,
    w=1 """
    
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    # Conteo total
    total_records = collection.count_documents({})
    print(f"Total de registros: {total_records:,}")
    
    # Genera patrones temporales
    voting_start, voting_end = generate_voting_timeframe(election_date, voting_hours)
    hourly_weights = create_hourly_distribution(voting_hours, end_concentration_pct)
    weighted_hours = create_weighted_hour_choices(hourly_weights)
    
    print(f"Periodo: {voting_start} a {voting_end}")
    
    # Procesa documentos
    updated_count = 0
    start_time = time.time()
    
    print("\n" + "="*70)
    print("ACTUALIZANDO TIMESTAMPS")
    print("="*70)
    
    cursor = collection.find({}, {"_id": 1}).batch_size(BATCH_SIZE)
    batch_operations = []
    
    for doc in cursor:
        # Genera timestamp ultra-optimizado
        loaded_at, voting_hour, blockchain_delay = generate_ultra_optimized_timestamp(
            voting_start, weighted_hours
        )
        
        # Prepara operacion con solo 3 campos
        update_doc = {
            "_id": doc["_id"],
            "loaded_at": loaded_at,
            "voting_hour": voting_hour,
            "blockchain_delay_seconds": blockchain_delay
        }
        
        batch_operations.append(update_doc)
        
        # Ejecuta batch
        if len(batch_operations) >= BATCH_SIZE:
            execute_batch_update(collection, batch_operations)
            updated_count += len(batch_operations)
            
            elapsed = time.time() - start_time
            rate = updated_count / elapsed if elapsed > 0 else 0
            progress = (updated_count / total_records) * 100
            
            print(f"Progreso: {updated_count:,}/{total_records:,} ({progress:.1f}%) "
                  f"| Tasa: {rate:,.0f} docs/seg")
            
            batch_operations = []
    
    # Batch restante
    if batch_operations:
        execute_batch_update(collection, batch_operations)
        updated_count += len(batch_operations)
    
    # Estadisticas finales
    total_time = time.time() - start_time
    
    print("\n" + "="*70)
    print("RESUMEN")
    print("="*70)
    print(f"Registros actualizados: {updated_count:,}")
    print(f"Tiempo total: {total_time/60:.2f} min ({total_time/3600:.2f} hrs)")
    print(f"Velocidad promedio: {updated_count/total_time:,.0f} docs/seg")
    
    create_temporal_indexes(collection)
    generate_distribution_report(collection, hourly_weights)
    
    client.close()
    print("\nProceso completado")


def execute_batch_update(collection, batch_operations):
    """
    Ejecuta operaciones de actualizacion en lote
    """
    operations = []
    for doc in batch_operations:
        doc_id = doc.pop("_id")
        operations.append(UpdateOne({"_id": doc_id}, {"$set": doc}))
    
    try:
        result = collection.bulk_write(operations, ordered=False)
        return result.modified_count
    except BulkWriteError as bwe:
        print(f"  Bulk error: {len(bwe.details.get('writeErrors', []))} errores")
        return bwe.details.get('nModified', 0)
    except Exception as e:
        print(f"  Error: {e}")
        return 0


def create_temporal_indexes(collection):
    """
    Crea indices para analisis temporal
    """
    print("\n" + "="*70)
    print("CREANDO INDICES")
    print("="*70)
    
    try:
        collection.create_index("loaded_at")
        print("  ✓ loaded_at")
        
        collection.create_index("voting_hour")
        print("  ✓ voting_hour")
        
        collection.create_index([("state", 1), ("voting_hour", 1)])
        print("  ✓ state + voting_hour")
        
        collection.create_index([("loaded_at", 1), ("state", 1)])
        print("  ✓ loaded_at + state")
        
        print("\nIndices creados")
    except Exception as e:
        print(f"  Error: {e}")


def generate_distribution_report(collection, expected_weights):
    """
    Genera reporte de distribucion horaria
    """
    print("\n" + "="*70)
    print("ANALISIS DE DISTRIBUCION")
    print("="*70)
    
    try:
        # Analiza distribucion usando voting_hour
        pipeline = [
            {"$addFields": {"hour_number": {"$toInt": {"$substr": ["$voting_hour", 0, 2]}}}},
            {"$group": {"_id": "$hour_number", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        
        hourly_results = list(collection.aggregate(pipeline))
        total_votes = sum(doc["count"] for doc in hourly_results)
        
        print(f"\nTotal: {total_votes:,}")
        print("\nHora | Cantidad | Real % | Esperado %")
        print("-" * 50)
        
        for doc in hourly_results:
            hour = doc["_id"]
            count = doc["count"]
            actual_pct = (count / total_votes) * 100
            expected_pct = expected_weights.get(hour, 0) * 100
            
            print(f"{hour:2d}   | {count:9,} | {actual_pct:6.2f}% | {expected_pct:8.2f}%")
        
    except Exception as e:
        print(f"  Error: {e}")


def main():
    """
    Funcion principal
    """
    print("\n" + "="*70)
    print("GENERADOR ULTRA-OPTIMIZADO DE TIMESTAMPS")
    print("="*70)
    
    try:
        update_voting_timestamps(
            election_date_str=ELECTION_DATE,
            voting_hours=VOTING_HOURS,
            end_concentration_pct=END_CONCENTRATION
        )
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
