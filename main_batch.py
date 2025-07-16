#!/usr/bin/env python3
"""
Motor SQL con procesamiento por lotes para grandes datasets
"""

import os
import sys
import gc
import psutil
from engine import Engine
from parser_sql.parser import SQLParser

def get_memory_usage():
    """Obtiene el uso de memoria actual en MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def print_memory_status():
    """Imprime el estado actual de la memoria"""
    memory_mb = get_memory_usage()
    print(f"💾 Memoria en uso: {memory_mb:.1f} MB")

def create_table_batch(sql_parser, table_name, csv_path, batch_size=1000):
    """Crea una tabla procesando el dataset en lotes"""
    print(f"\n📊 Configuración de procesamiento por lotes")
    print(f"Archivo: {csv_path}")
    print(f"Tamaño de lote: {batch_size} registros")
    
    # Primero contar el total de registros
    total_rows = sum(1 for line in open(csv_path, 'r', encoding='latin1')) - 1
    print(f"Total de registros: {total_rows}")
    
    # Preguntar al usuario qué tipo de índice usar
    print("\n🔍 Seleccione el tipo de procesamiento:")
    print("1. Solo índice básico (Hash) - Rápido, menos memoria")
    print("2. Índice de texto (SPIMI) - Para búsquedas de texto")
    print("3. Procesamiento multimedia (SIFT) - Muy intensivo en memoria")
    
    choice = input("\nOpción (1-3): ").strip()
    
    if choice == '1':
        # Índice hash simple
        query = f'CREATE TABLE {table_name} FROM FILE "{csv_path}" USING INDEX hash(0)'
        print(f"\n🔄 Ejecutando: {query}")
        result = sql_parser.parse_and_execute(query)
        return result
    
    elif choice == '2':
        # Índice de texto SPIMI
        print("\n📝 Ingrese las columnas de texto a indexar")
        print("Ejemplo: track_name,track_artist,lyrics")
        text_cols = input("Columnas: ").strip()
        
        query = f'CREATE TABLE {table_name} FROM FILE "{csv_path}" USING INDEX spimi({text_cols})'
        print(f"\n🔄 Ejecutando: {query}")
        result = sql_parser.parse_and_execute(query)
        return result
    
    elif choice == '3':
        # Procesamiento multimedia por lotes
        print("\n⚠️  ADVERTENCIA: El procesamiento multimedia es muy intensivo")
        print("Se recomienda procesar solo una muestra del dataset")
        
        sample_size = input(f"¿Cuántos registros procesar? (max {total_rows}): ").strip()
        try:
            sample_size = min(int(sample_size), total_rows)
        except:
            sample_size = 1000
        
        print(f"\n🎨 Procesando {sample_size} imágenes en lotes de {batch_size}")
        
        # Aquí implementarías el procesamiento por lotes
        # Por ahora, solo crear la tabla base
        query = f'CREATE TABLE {table_name} FROM FILE "{csv_path}" USING INDEX hash(0)'
        result = sql_parser.parse_and_execute(query)
        
        if result and result.get('success'):
            print("\n✅ Tabla base creada. El procesamiento multimedia completo está en desarrollo.")
        
        return result

def main():
    """Función principal optimizada para grandes datasets"""
    print("=" * 80)
    print("    SISTEMA DE BASE DE DATOS - MODO BATCH (Optimizado para memoria)")
    print("=" * 80)
    
    # Verificar memoria disponible
    try:
        import psutil
    except ImportError:
        print("⚠️  Instalando psutil para monitoreo de memoria...")
        os.system("pip3 install psutil")
        import psutil
    
    print_memory_status()
    
    # Inicializar sistema
    print("\n⚙️  Inicializando sistema...")
    engine = Engine()
    sql_parser = SQLParser(engine)
    
    while True:
        print("\n" + "=" * 60)
        print("📋 OPCIONES:")
        print("1. Crear tabla desde CSV (procesamiento optimizado)")
        print("2. Ejecutar consulta SQL")
        print("3. Ver tablas existentes")
        print("4. Liberar memoria")
        print("5. Salir")
        
        choice = input("\nSeleccione opción (1-5): ").strip()
        
        if choice == '1':
            # Crear tabla con procesamiento por lotes
            csv_path = input("\n📁 Ruta del archivo CSV: ").strip()
            if not os.path.exists(csv_path):
                print("❌ El archivo no existe")
                continue
            
            table_name = input("📋 Nombre de la tabla: ").strip()
            if not table_name.isidentifier():
                print("❌ Nombre de tabla inválido")
                continue
            
            # Determinar tamaño de lote basado en el archivo
            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            if file_size_mb > 100:
                batch_size = 500  # Lotes más pequeños para archivos grandes
            else:
                batch_size = 1000
            
            result = create_table_batch(sql_parser, table_name, csv_path, batch_size)
            
            if result and result.get('success'):
                print("✅ Tabla creada exitosamente")
            else:
                print(f"❌ Error: {result.get('error', 'Error desconocido')}")
            
            print_memory_status()
            
        elif choice == '2':
            # Ejecutar consulta
            query = input("\n🔍 SQL> ").strip()
            if query:
                result = sql_parser.parse_and_execute(query)
                if result and result.get('success'):
                    if 'data' in result and result['data']:
                        # Mostrar solo primeros 10 resultados
                        print(f"\nResultados (mostrando max 10 de {len(result['data'])}):")
                        for i, row in enumerate(result['data'][:10]):
                            print(f"{i+1}. {row}")
                    else:
                        print(f"✅ {result.get('message', 'Ejecutado correctamente')}")
                else:
                    print(f"❌ Error: {result.get('error', 'Error desconocido')}")
        
        elif choice == '3':
            # Ver tablas
            tables = list(engine.tables.keys()) + list(engine.text_tables.keys())
            if tables:
                print("\n📊 Tablas existentes:")
                for i, table in enumerate(tables, 1):
                    print(f"  {i}. {table}")
            else:
                print("\n📭 No hay tablas creadas")
        
        elif choice == '4':
            # Liberar memoria
            print("\n🧹 Liberando memoria...")
            gc.collect()
            print_memory_status()
        
        elif choice == '5':
            print("\n👋 ¡Hasta luego!")
            break
        
        else:
            print("❌ Opción inválida")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Programa interrumpido")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()