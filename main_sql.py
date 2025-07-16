#!/usr/bin/env python3
"""
Motor SQL interactivo para el Sistema de Base de Datos Multimedia
"""

import os
import sys
import time
from engine import Engine
from parser_sql.parser import SQLParser
try:
    from tabulate import tabulate
    TABULATE_AVAILABLE = True
except ImportError:
    TABULATE_AVAILABLE = False
    print("⚠️  Instalando tabulate para mejor visualización...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tabulate"])
    try:
        from tabulate import tabulate
        TABULATE_AVAILABLE = True
    except:
        TABULATE_AVAILABLE = False

import pandas as pd

def clear_screen():
    """Limpia la pantalla de la consola"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    """Imprime el encabezado del sistema"""
    clear_screen()
    print("=" * 80)
    print("         SISTEMA DE BASE DE DATOS MULTIMEDIA - MOTOR SQL")
    print("=" * 80)
    print()

def print_help():
    """Muestra ayuda de comandos"""
    help_text = """
📖 COMANDOS DISPONIBLES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔧 CREACIÓN DE TABLAS:
  CREATE TABLE tabla FROM FILE "archivo.csv" USING INDEX tipo(columna)
  
  Tipos de índice:
  • hash(col)     - Búsqueda exacta rápida
  • btree(col)    - Búsqueda por rango
  • rtree         - Búsqueda espacial
  • inverted("col1", "col2") - Búsqueda de texto
  • spimi("col1", "col2")    - Búsqueda de texto para grandes volúmenes

🔍 CONSULTAS:
  • SELECT * FROM tabla WHERE columna = 'valor'
  • SELECT * FROM tabla WHERE columna BETWEEN 'val1' AND 'val2'
  • SELECT * FROM tabla WHERE texto @@ 'palabra'
  • SELECT * FROM tabla WHERE location <-> '(lat,lon)' < radio
  • SELECT * FROM tabla ORDER BY columna LIMIT n

🎨 MULTIMEDIA (próximamente):
  • CREATE MULTIMEDIA INDEX ON tabla USING sift FOR images
  • SELECT * FROM tabla WHERE image_similarity('query.jpg') > 0.8

📊 INFORMACIÓN:
  • SHOW TABLES       - Lista todas las tablas
  • DESCRIBE tabla    - Muestra estructura de la tabla
  • SHOW INDEX tabla  - Muestra índices de la tabla

💾 UTILIDADES:
  • CLEAR       - Limpia la pantalla
  • HELP        - Muestra esta ayuda
  • EXIT/QUIT   - Salir del programa

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    print(help_text)

def format_results(results, limit=10):
    """Formatea los resultados en una tabla bonita"""
    if not results or not results.get('data'):
        return "Sin resultados"
    
    data = results['data']
    
    # Si es una lista de listas (filas de datos)
    if isinstance(data[0], list):
        # Intentar obtener headers si están disponibles
        headers = results.get('headers', [f"Col{i}" for i in range(len(data[0]))])
        
        # Limitar resultados mostrados
        if len(data) > limit:
            display_data = data[:limit]
            footer = f"\n... mostrando {limit} de {len(data)} filas totales"
        else:
            display_data = data
            footer = f"\n{len(data)} filas en total"
        
        if TABULATE_AVAILABLE:
            table = tabulate(display_data, headers=headers, tablefmt='grid', showindex=True)
        else:
            # Formato simple sin tabulate
            table = " | ".join(headers) + "\n"
            table += "-" * (len(table) - 1) + "\n"
            for row in display_data:
                table += " | ".join(str(cell)[:30] for cell in row) + "\n"
        
        return table + footer
    
    # Si es una lista de strings o mensajes
    elif isinstance(data, list) and all(isinstance(item, str) for item in data):
        return "\n".join(data)
    
    # Otro formato
    else:
        return str(data)

def show_query_examples(tables):
    """Muestra ejemplos de consultas según las tablas existentes"""
    if not tables:
        print("ℹ️  No hay tablas creadas aún.")
        return
    
    print("\n📝 EJEMPLOS DE CONSULTAS:")
    print("=" * 60)
    
    for table_name, table_info in tables.items():
        print(f"\n🔸 Tabla: {table_name}")
        
        # Determinar tipo de índice
        if hasattr(table_info, '__class__'):
            index_type = table_info.__class__.__name__
        else:
            index_type = "Unknown"
        
        # Mostrar ejemplos según el tipo
        if "Hash" in index_type:
            print(f"  SELECT * FROM {table_name} WHERE column_name = 'value'")
        elif "BPlusTree" in index_type:
            print(f"  SELECT * FROM {table_name} WHERE column_name = 'value'")
            print(f"  SELECT * FROM {table_name} WHERE column_name BETWEEN 'A' AND 'Z'")
        elif "RTree" in index_type:
            print(f"  SELECT * FROM {table_name} WHERE location <-> '(40.7, -74.0)' < 10")
            print(f"  SELECT * FROM {table_name} ORDER BY location <-> '(40.7, -74.0)' LIMIT 5")
    
    # Si hay tablas de texto
    if any('text_tables' in str(type(t)) for t in tables.values()):
        print("\n🔸 Búsquedas de texto:")
        print("  SELECT * FROM tabla WHERE campo @@ 'palabra'")
        print("  SELECT * FROM tabla WHERE campo @@ 'frase completa' LIMIT 10")

def execute_special_command(command, sql_parser):
    """Ejecuta comandos especiales del sistema"""
    cmd = command.upper().strip()
    
    if cmd == "CLEAR":
        clear_screen()
        return True
    elif cmd == "HELP":
        print_help()
        return True
    elif cmd == "SHOW TABLES":
        tables = []
        # Tablas regulares
        for name in sql_parser.engine.tables.keys():
            tables.append([name, "Regular", "Active"])
        # Tablas de texto
        for name in sql_parser.engine.text_tables.keys():
            tables.append([name, "Text/SPIMI", "Active"])
        
        if tables:
            if TABULATE_AVAILABLE:
                print(tabulate(tables, headers=["Table Name", "Type", "Status"], tablefmt='grid'))
            else:
                print("Table Name | Type | Status")
                print("-" * 40)
                for row in tables:
                    print(" | ".join(row))
        else:
            print("No hay tablas creadas.")
        return True
    elif cmd.startswith("DESCRIBE "):
        table_name = cmd.replace("DESCRIBE ", "").strip()
        # Aquí podrías implementar la lógica para mostrar la estructura
        print(f"Estructura de la tabla {table_name}:")
        print("(Funcionalidad en desarrollo)")
        return True
    
    return False

def create_sample_queries():
    """Crea un archivo con consultas de ejemplo"""
    queries = """-- Ejemplos de consultas SQL para el Sistema de Base de Datos Multimedia

-- 1. Crear tabla con índice Hash
CREATE TABLE productos FROM FILE "datos/productos.csv" USING INDEX hash(0)

-- 2. Crear tabla con índice B+Tree
CREATE TABLE empleados FROM FILE "datos/empleados.csv" USING INDEX btree(1)

-- 3. Crear tabla con índice R-Tree (para datos geoespaciales)
CREATE TABLE ubicaciones FROM FILE "datos/ubicaciones.csv" USING INDEX rtree

-- 4. Crear tabla con índice de texto SPIMI
CREATE TABLE canciones FROM FILE "datos/spotify_songs.csv" USING INDEX spimi("track_name", "track_artist", "lyrics")

-- 5. Búsquedas simples
SELECT * FROM productos WHERE id = 100
SELECT * FROM empleados WHERE salario BETWEEN 50000 AND 100000

-- 6. Búsquedas de texto
SELECT * FROM canciones WHERE lyrics @@ 'love'
SELECT * FROM canciones WHERE track_name @@ 'hello' LIMIT 10

-- 7. Búsquedas espaciales
SELECT * FROM ubicaciones WHERE location <-> '(40.7128, -74.0060)' < 5
SELECT * FROM ubicaciones ORDER BY location <-> '(40.7128, -74.0060)' LIMIT 10
"""
    
    with open("consultas_ejemplo.sql", "w") as f:
        f.write(queries)
    print("✅ Archivo 'consultas_ejemplo.sql' creado con ejemplos de consultas.")

def main():
    """Función principal del motor SQL"""
    print_header()
    
    print("🚀 Iniciando Motor SQL Multimedia...")
    print("💡 Escribe 'help' para ver comandos disponibles\n")
    
    # Inicializar el sistema
    engine = Engine()
    sql_parser = SQLParser(engine)
    
    # Historial de consultas
    query_history = []
    
    # Crear archivo de consultas de ejemplo
    if not os.path.exists("consultas_ejemplo.sql"):
        create_sample_queries()
    
    print("✅ Sistema iniciado correctamente")
    print("━" * 80)
    
    # Loop principal
    while True:
        try:
            # Prompt mejorado
            prompt = "\n🔍 SQL> "
            query = input(prompt).strip()
            
            # Comandos de salida
            if query.lower() in ['exit', 'quit', 'salir', 'q']:
                print("\n👋 ¡Hasta luego!")
                break
            
            # Ignorar líneas vacías
            if not query:
                continue
            
            # Agregar al historial
            query_history.append(query)
            
            # Verificar comandos especiales
            if execute_special_command(query, sql_parser):
                continue
            
            # Medir tiempo de ejecución
            start_time = time.time()
            
            # Ejecutar consulta SQL
            print(f"\n⚡ Ejecutando: {query}")
            print("─" * 60)
            
            result = sql_parser.parse_and_execute(query)
            
            # Calcular tiempo transcurrido
            elapsed_time = time.time() - start_time
            
            # Mostrar resultados
            if result and result.get('success'):
                if 'data' in result and result['data']:
                    print(format_results(result))
                    print(f"\n⏱️  Tiempo de ejecución: {elapsed_time:.3f} segundos")
                else:
                    message = result.get('message', 'Consulta ejecutada exitosamente')
                    print(f"✅ {message}")
                    print(f"⏱️  Tiempo de ejecución: {elapsed_time:.3f} segundos")
            else:
                error = result.get('error', 'Error desconocido') if result else 'Error en la ejecución'
                print(f"❌ Error: {error}")
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Consulta interrumpida. Use 'exit' para salir.")
            continue
        except Exception as e:
            print(f"\n❌ Error inesperado: {str(e)}")
            import traceback
            if input("¿Ver detalles del error? (s/n): ").lower() == 's':
                traceback.print_exc()

    # Guardar historial antes de salir
    if query_history:
        save_history = input("\n¿Guardar historial de consultas? (s/n): ").lower()
        if save_history == 's':
            with open("historial_consultas.sql", "w") as f:
                f.write("-- Historial de consultas\n")
                f.write(f"-- Fecha: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                for i, q in enumerate(query_history, 1):
                    f.write(f"-- [{i}] {q}\n")
            print("✅ Historial guardado en 'historial_consultas.sql'")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error fatal: {str(e)}")
        import traceback
        traceback.print_exc()