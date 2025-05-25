#!/usr/bin/env python3
"""
Sistema de Base de Datos Multimodal
Punto de entrada principal del sistema
"""

import sys
import os
from engine import Engine
from parser_sql.parser import SQLParser

def demo_sql_queries(engine, parser):
    """
    Demostración de consultas SQL básicas
    """
    print("\n" + "="*60)
    print("DEMO DE CONSULTAS SQL")
    print("="*60)
    
    # Verificar si hay datos de ejemplo
    sample_files = [
        "datos/StudentsPerformance.csv",
        "datos/powerplants.csv",
        "datos/kcdatahouse.csv"
    ]
    
    # Buscar un archivo que exista
    sample_file = None
    for file in sample_files:
        if os.path.exists(file):
            sample_file = file
            break
    
    if not sample_file:
        print("No se encontraron archivos de datos de ejemplo.")
        print("Asegúrate de tener archivos CSV en la carpeta 'datos/'")
        return
    
    print(f"Usando archivo de ejemplo: {sample_file}")
    
    try:
        # 1. Crear tabla
        print("\n1. Creando tabla con índice B+ Tree...")
        create_query = f'create table students from file "{sample_file}" using index btree("id")'
        print(f"Query: {create_query}")
        result = parser.parse_and_execute(create_query)
        print(f"Resultado: {result}")
        
        # 2. Escanear tabla
        print("\n2. Escaneando tabla completa...")
        scan_query = "select * from students"
        print(f"Query: {scan_query}")
        results = parser.parse_and_execute(scan_query)
        print(f"Primeros 3 registros:")
        for i, record in enumerate(results[:3]):
            print(f"  {i+1}: {record}")
        print(f"Total de registros: {len(results)}")
        
        # 3. Búsqueda exacta
        print("\n3. Búsqueda por clave exacta...")
        if results:
            # Tomar el primer valor de la primera columna para buscar
            first_record = results[0].split('|')
            search_value = first_record[0].strip()
            search_query = f"select * from students where id = {search_value}"
            print(f"Query: {search_query}")
            search_results = parser.parse_and_execute(search_query)
            print(f"Registros encontrados: {len(search_results)}")
            for record in search_results:
                print(f"  {record}")
        
        # 4. Insertar registro
        print("\n4. Insertando nuevo registro...")
        insert_query = 'insert into students values ("999", "Test Student", "male", "group A", "some school", "standard", "free/reduced", "completed", "85", "90", "88")'
        print(f"Query: {insert_query}")
        result = parser.parse_and_execute(insert_query)
        print(f"Resultado: {result}")
        
        # 5. Búsqueda del registro insertado
        print("\n5. Verificando registro insertado...")
        verify_query = "select * from students where id = 999"
        print(f"Query: {verify_query}")
        verify_results = parser.parse_and_execute(verify_query)
        print(f"Registros encontrados: {len(verify_results)}")
        for record in verify_results:
            print(f"  {record}")
        
    except Exception as e:
        print(f"Error en demo: {e}")

def interactive_mode(engine, parser):
    """
    Modo interactivo para ejecutar consultas SQL
    """
    print("\n" + "="*60)
    print("MODO INTERACTIVO SQL")
    print("="*60)
    print("Escribe 'help' para ver comandos disponibles")
    print("Escribe 'exit' para salir")
    print("Escribe 'tables' para ver las tablas disponibles")
    
    while True:
        try:
            query = input("\nSQL> ").strip()
            
            if query.lower() == 'exit':
                print("¡Hasta luego!")
                break
            elif query.lower() == 'help':
                print_help()
                continue
            elif query.lower() == 'tables':
                print("Tablas disponibles:")
                for table_name in engine.tables.keys():
                    print(f"  - {table_name}")
                continue
            elif not query:
                continue
            
            # Validar sintaxis
            if not parser.validate_syntax(query):
                print(f"❌ Sintaxis inválida")
                print(f"💡 Sugerencia: {parser.suggest_correction(query)}")
                continue
            
            # Ejecutar consulta
            print(f"🔄 Ejecutando: {query}")
            result = parser.parse_and_execute(query)
            
            # Mostrar resultado
            if isinstance(result, list):
                print(f"✅ Se encontraron {len(result)} registros:")
                for i, record in enumerate(result[:10]):  # Mostrar máximo 10
                    print(f"  {i+1}: {record}")
                if len(result) > 10:
                    print(f"  ... y {len(result) - 10} registros más")
            else:
                print(f"✅ {result}")
                
        except KeyboardInterrupt:
            print("\n\n¡Hasta luego!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

def print_help():
    """
    Muestra ayuda sobre los comandos disponibles
    """
    help_text = """
COMANDOS DISPONIBLES:

📁 CREAR TABLA:
   create table <nombre> from file "<ruta>" using index <tipo>("<columna>")
   
   Tipos de índice: sequential, isam, hash, btree, rtree
   Ejemplo: create table students from file "datos/students.csv" using index btree("id")

🔍 CONSULTAS SELECT:
   select * from <tabla>                              # Todos los registros
   select * from <tabla> where <col> = <valor>        # Búsqueda exacta
   select * from <tabla> where <col> between <a> and <b>  # Rango
   select * from <tabla> where <col> in (<punto>, <radio>)  # Espacial (solo R-Tree)

➕ INSERTAR:
   insert into <tabla> values (<val1>, <val2>, ...)
   
❌ ELIMINAR:
   delete from <tabla> where <col> = <valor>

🔧 COMANDOS ESPECIALES:
   help     - Mostrar esta ayuda
   tables   - Listar tablas disponibles
   exit     - Salir del programa
"""
    print(help_text)

def main():
    """
    Función principal del sistema
    """
    print("🚀 Sistema de Base de Datos Multimodal")
    print("=====================================")
    
    # Inicializar motor y parser
    engine = Engine()
    parser = SQLParser(engine)
    
    # Verificar argumentos de línea de comandos
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == 'api':
            # Iniciar API
            print("🌐 Iniciando API FastAPI...")
            try:
                import uvicorn
                from backend.api import app
                uvicorn.run(app, host="0.0.0.0", port=8000)
            except ImportError:
                print("❌ Error: FastAPI no está instalado.")
                print("💡 Instala las dependencias: pip install -r requirements.txt")
            except Exception as e:
                print(f"❌ Error iniciando API: {e}")
        
        elif mode == 'demo':
            # Ejecutar demo
            demo_sql_queries(engine, parser)
        
        elif mode == 'interactive':
            # Modo interactivo
            interactive_mode(engine, parser)
        
        else:
            print(f"❌ Modo no reconocido: {mode}")
            print("Modos disponibles: api, demo, interactive")
    
    else:
        # Mostrar opciones disponibles
        print("\nOpciones disponibles:")
        print("  python main.py api         # Iniciar API FastAPI")
        print("  python main.py demo        # Ejecutar demo con consultas")
        print("  python main.py interactive # Modo interactivo SQL")
        print("\nO simplemente ejecuta main.py para ver este menú")
        
        # Preguntar al usuario qué quiere hacer
        print("\n¿Qué deseas hacer?")
        print("1. Iniciar API FastAPI")
        print("2. Ejecutar demo")
        print("3. Modo interactivo SQL")
        print("4. Salir")
        
        try:
            choice = input("\nSelecciona una opción (1-4): ").strip()
            
            if choice == '1':
                try:
                    import uvicorn
                    from backend.api import app
                    print("🌐 Iniciando API en http://localhost:8000")
                    print("📖 Documentación disponible en http://localhost:8000/docs")
                    uvicorn.run(app, host="0.0.0.0", port=8000)
                except ImportError:
                    print("❌ Error: FastAPI no está instalado.")
                    print("💡 Instala las dependencias: pip install -r requirements.txt")
            elif choice == '2':
                demo_sql_queries(engine, parser)
            elif choice == '3':
                interactive_mode(engine, parser)
            elif choice == '4':
                print("¡Hasta luego!")
            else:
                print("❌ Opción no válida")
                
        except KeyboardInterrupt:
            print("\n\n¡Hasta luego!")

if __name__ == "__main__":
    main()