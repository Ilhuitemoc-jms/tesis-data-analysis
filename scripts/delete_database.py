# scripts/drop_database.py
"""
Script simple para eliminar completamente una base de datos de MongoDB
"""

from pymongo import MongoClient


# MongoDB Configuration
MONGO_URI = "mongodb://admin:votaciones2025@localhost:27017/"
DATABASE_NAME = "votaciones_usa"


def drop_database(database_name):
    """
    Elimina completamente una base de datos de MongoDB
    
    Args:
        database_name (str): Nombre de la base de datos a eliminar
    """
    print("="*60)
    print("ELIMINACION DE BASE DE DATOS")
    print("="*60)
    print(f"Base de datos a eliminar: {database_name}")
    print("\nConectando a MongoDB...")
    
    try:
        # Conecta al servidor MongoDB
        client = MongoClient(MONGO_URI)
        
        # Lista todas las bases de datos disponibles
        existing_databases = client.list_database_names()
        print(f"\nBases de datos existentes: {existing_databases}")
        
        # Verifica si la base de datos existe
        if database_name in existing_databases:
            print(f"\n¿Esta seguro que desea eliminar la base '{database_name}'?")
            confirmacion = input("Escriba 'SI' para confirmar: ")
            
            if confirmacion.upper() == "SI":
                # Elimina la base de datos
                client.drop_database(database_name)
                print(f"\n✓ Base de datos '{database_name}' eliminada exitosamente")
                
                # Verifica que se elimino
                remaining_databases = client.list_database_names()
                print(f"\nBases de datos restantes: {remaining_databases}")
                
                if database_name not in remaining_databases:
                    print(f"\n✓ Confirmado: '{database_name}' ya no existe")
                else:
                    print(f"\n✗ Error: '{database_name}' todavia existe")
            else:
                print("\nOperacion cancelada por el usuario")
        else:
            print(f"\n✗ La base de datos '{database_name}' no existe")
            print("No hay nada que eliminar")
        
        # Cierra la conexion
        client.close()
        
    except Exception as e:
        print(f"\n✗ Error al conectar o eliminar la base de datos: {e}")
        return 1
    
    print("\n" + "="*60)
    return 0


def main():
    """
    Funcion principal de ejecucion
    """
    print("Script para Eliminar Base de Datos MongoDB")
    print("\nADVERTENCIA: Esta operacion es IRREVERSIBLE")
    print("Todos los datos, colecciones e indices seran eliminados\n")
    
    try:
        return drop_database(DATABASE_NAME)
    except KeyboardInterrupt:
        print("\n\nOperacion cancelada por el usuario")
        return 1


if __name__ == "__main__":
    exit(main())
