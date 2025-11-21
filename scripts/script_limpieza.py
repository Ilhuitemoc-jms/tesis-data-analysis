# scripts/cleanup_timestamp_fields.py
"""
Script para eliminar campos de timestamp creados por scripts anteriores
Ejecuta esto antes de correr el nuevo script optimizado
"""

import time
from pymongo import MongoClient, UpdateMany


# Configuracion de MongoDB
MONGO_URI = "mongodb://admin:votaciones2025@mongodb:27017/"
DATABASE_NAME = "votaciones_usa"
COLLECTION_NAME = "registros_votacion"

# Batch size para actualizaciones
BATCH_SIZE = 50000


def cleanup_timestamp_fields():
    """
    Elimina todos los campos de timestamp generados previamente
    """
    print("="*70)
    print("LIMPIEZA DE CAMPOS DE TIMESTAMP")
    print("="*70)
    
    # Conecta a MongoDB
    print("\nConectando a MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    # Verifica cuantos documentos tienen estos campos
    total_docs = collection.count_documents({})
    docs_with_loaded_at = collection.count_documents({"loaded_at": {"$exists": True}})
    docs_with_voting_hour = collection.count_documents({"voting_hour": {"$exists": True}})
    
    print(f"\nEstado actual:")
    print(f"  Total documentos: {total_docs:,}")
    print(f"  Con loaded_at: {docs_with_loaded_at:,}")
    print(f"  Con voting_hour: {docs_with_voting_hour:,}")
    
    if docs_with_loaded_at == 0 and docs_with_voting_hour == 0:
        print("\nNo hay campos de timestamp para eliminar")
        client.close()
        return
    
    # Lista de campos a eliminar
    fields_to_remove = {
        "loaded_at": "",
        "voting_hour": "",
        "voting_date": "",
        "is_early_vote": "",
        "blockchain_delay_seconds": ""
    }
    
    print("\nCampos a eliminar:")
    for field in fields_to_remove.keys():
        print(f"  - {field}")
    
    # Confirma la operacion
    print("\n" + "="*70)
    print("ADVERTENCIA: Esta operacion eliminara estos campos de TODOS los documentos")
    print("="*70)
    
    # Ejecuta la eliminacion
    print("\nEliminando campos...")
    start_time = time.time()
    
    try:
        # Usa update_many con $unset para eliminar campos
        result = collection.update_many(
            {},
            {"$unset": fields_to_remove}
        )
        
        elapsed_time = time.time() - start_time
        
        print("\n" + "="*70)
        print("RESULTADO")
        print("="*70)
        print(f"Documentos modificados: {result.modified_count:,}")
        print(f"Tiempo: {elapsed_time:.2f} segundos ({elapsed_time/60:.2f} minutos)")
        
        # Verifica que se eliminaron
        remaining_with_loaded_at = collection.count_documents({"loaded_at": {"$exists": True}})
        remaining_with_voting_hour = collection.count_documents({"voting_hour": {"$exists": True}})
        
        print(f"\nVerificacion:")
        print(f"  Documentos con loaded_at restantes: {remaining_with_loaded_at:,}")
        print(f"  Documentos con voting_hour restantes: {remaining_with_voting_hour:,}")
        
        if remaining_with_loaded_at == 0 and remaining_with_voting_hour == 0:
            print("\n✓ Limpieza completada exitosamente")
        else:
            print("\n⚠ Algunos documentos aun tienen campos de timestamp")
        
    except Exception as e:
        print(f"\nError durante la limpieza: {e}")
    
    client.close()
    print("\nProceso completado")


def main():
    """
    Funcion principal
    """
    print("\n" + "="*70)
    print("SCRIPT DE LIMPIEZA DE CAMPOS TIMESTAMP")
    print("="*70)
    print("\nEste script elimina campos creados por scripts de timestamp anteriores")
    print("Campos a eliminar:")
    print("  - loaded_at")
    print("  - voting_hour")
    print("  - voting_date")
    print("  - is_early_vote")
    print("  - blockchain_delay_seconds")
    
    try:
        cleanup_timestamp_fields()
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
