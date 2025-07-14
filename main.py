# main.py - Versión Completa con Parser SQL Multimedia
import os
import sys
import time
from typing import Optional
from engine import Engine # Tu engine original
from multimedia.multimedia_engine import MultimediaEngine

def print_main_menu():
    """Menú principal para elegir tipo de operación"""
    print("\n" + "="*60)
    print("           SISTEMA DE BASE DE DATOS MULTIMODAL")
    print("="*60)
    print("¿Qué tipo de operación deseas realizar?")
    print()
    print("1. ÍNDICES TRADICIONALES")
    print("   - Sequential, Hash, B-Tree, ISAM, R-Tree")
    print("   - Búsqueda textual con SPIMI")
    print("   - Operaciones SQL básicas")
    print()
    print("2. BÚSQUEDA MULTIMEDIA")
    print("   - Imágenes (SIFT, ResNet50, InceptionV3)")
    print("   - Audio (MFCC, Spectrogram)")
    print("   - Búsqueda por similitud")
    print()
    print("3. PARSER SQL MULTIMEDIA")
    print("   - Consultas SQL extendidas")
    print("   - Sintaxis: SELECT ... WHERE campo <-> 'archivo'")
    print("   - Soporte para texto y multimedia")
    print()
    print("4. INFORMACIÓN DEL SISTEMA")
    print("5. SALIR")
    print("="*60)

def print_traditional_menu():
    """Menú para índices tradicionales"""
    print("\n" + "="*50)
    print("         ÍNDICES TRADICIONALES")
    print("="*50)
    print("1.  Cargar CSV con índice")
    print("2.  Crear índice textual SPIMI")
    print("3.  Insertar registro")
    print("4.  Buscar por clave")
    print("5.  Búsqueda por rango")
    print("6.  Búsqueda textual")
    print("7.  Eliminar registro")
    print("8.  Mostrar toda la tabla")
    print("9.  Información de tablas")
    print("0.  Volver al menú principal")
    print("="*50)

def print_multimedia_menu():
    """Menú para multimedia"""
    print("\n" + "="*50)
    print("         BÚSQUEDA MULTIMEDIA")
    print("="*50)
    print("1.  Configurar sistema multimedia")
    print("2.  Construir índice multimedia")
    print("3.  Buscar por similitud")
    print("4.  Benchmark de rendimiento")
    print("5.  Ejemplo completo")
    print("6.  Estadísticas del sistema")
    print("0.  Volver al menú principal")
    print("="*50)

def print_sql_parser_menu():
    """Menú para parser SQL multimedia"""
    print("\n" + "="*60)
    print("         PARSER SQL MULTIMEDIA")
    print("="*60)
    print("1. Crear tabla tradicional (SQL)")
    print("2. Crear tabla textual (SPIMI)")
    print("3. Crear tabla multimedia")
    print("4. Búsqueda textual (@@)")
    print("5. Búsqueda multimedia (<->)")
    print("6. Mostrar tablas")
    print("7. Ejemplos de sintaxis")
    print("8. Modo interactivo SQL")
    print("0.  Volver al menú principal")
    print("="*60)

# ========================================
# NUEVAS FUNCIONES PARA PARSER SQL
# ========================================

def handle_sql_parser():
    """Maneja operaciones con el parser SQL multimedia"""
    from parser_sql.parser import SQLParser
    
    engine = Engine()
    parser = SQLParser(engine)
    
    while True:
        try:
            print_sql_parser_menu()
            choice = input("Selecciona una opción: ").strip()
            
            if choice == '0':
                break
            elif choice == '1':
                create_traditional_table_sql(parser)
            elif choice == '2':
                create_textual_table_sql(parser)
            elif choice == '3':
                create_multimedia_table_sql(parser)
            elif choice == '4':
                textual_search_sql(parser)
            elif choice == '5':
                multimedia_search_sql(parser)
            elif choice == '6':
                show_all_tables_sql(parser)
            elif choice == '7':
                show_sql_syntax_examples()
            elif choice == '8':
                interactive_sql_mode(parser)
            else:
                print("Error: Opción inválida")
                
        except KeyboardInterrupt:
            print("\n Volviendo al menú principal...")
            break
        except Exception as e:
            print(f"Error: {e}")

def create_traditional_table_sql(parser):
    """Crear tabla tradicional usando SQL"""
    print("\n--- Crear Tabla Tradicional (SQL) ---")
    print("Sintaxis: CREATE TABLE nombre FROM FILE \"archivo.csv\" USING INDEX tipo(campo);")
    print("Tipos: sequential, hash, btree, isam, rtree")
    print()
    
    # Mostrar archivos disponibles
    show_available_files()
    
    sql_query = input("Ingresa la consulta SQL: ").strip()
    if sql_query:
        try:
            result = parser.parse_and_execute(sql_query)
            print(f" {result}")
        except Exception as e:
            print(f"Error: {e}")

def create_textual_table_sql(parser):
    """Crear tabla textual usando SQL"""
    print("\n--- Crear Tabla Textual (SPIMI) ---")
    print("Sintaxis: CREATE TABLE nombre FROM FILE \"archivo.csv\" USING INDEX spimi(\"campo1\", \"campo2\");")
    print()
    
    show_available_files()
    
    sql_query = input("Ingresa la consulta SQL: ").strip()
    if sql_query:
        try:
            result = parser.parse_and_execute(sql_query)
            print(f" {result}")
        except Exception as e:
            print(f"Error: {e}")

def create_multimedia_table_sql(parser):
    """Crear tabla multimedia usando SQL"""
    print("\n--- Crear Tabla Multimedia ---")
    print("Sintaxis: CREATE MULTIMEDIA TABLE nombre FROM FILE \"archivo.csv\" USING tipo WITH METHOD método CLUSTERS n;")
    print("Tipos: image, audio")
    print("Métodos imagen: sift, resnet50, inception_v3")
    print("Métodos audio: mfcc, spectrogram, comprehensive")
    print()
    
    show_available_files()
    
    print("Ejemplo:")
    print('CREATE MULTIMEDIA TABLE fashion FROM FILE "datos/fashion_multimedia.csv" USING image WITH METHOD sift CLUSTERS 64;')
    print()
    
    sql_query = input("Ingresa la consulta SQL: ").strip()
    if sql_query:
        try:
            result = parser.parse_and_execute(sql_query)
            print(f" {result}")
        except Exception as e:
            print(f"Error: {e}")

def textual_search_sql(parser):
    """Búsqueda textual usando SQL"""
    print("\n--- Búsqueda Textual (@@) ---")
    print("Sintaxis: SELECT campos FROM tabla WHERE campo @@ 'consulta' LIMIT k;")
    print()
    
    # Mostrar tablas textuales disponibles
    tables = list(parser.engine.text_tables.keys())
    if tables:
        print("Tablas textuales disponibles:")
        for table in tables:
            print(f"  - {table}")
    else:
        print("Warning: No hay tablas textuales. Crea una primero.")
        return
    
    print("\nEjemplos:")
    print('SELECT * FROM musica WHERE lyrics @@ "love song" LIMIT 5;')
    print('SELECT track_name, track_artist FROM musica WHERE combined_text @@ "rock music" LIMIT 10;')
    print()
    
    sql_query = input("Ingresa la consulta SQL: ").strip()
    if sql_query:
        try:
            results = parser.parse_and_execute(sql_query)
            if results:
                print(f"\n Resultados encontrados: {len(results)}")
                for i, result in enumerate(results[:10], 1):  # Mostrar primeros 10
                    print(f"{i}. {result}")
                if len(results) > 10:
                    print(f"... y {len(results) - 10} más")
            else:
                print("Warning: No se encontraron resultados")
        except Exception as e:
            print(f"Error: {e}")

def multimedia_search_sql(parser):
    """Búsqueda multimedia usando SQL"""
    print("\n--- Búsqueda Multimedia (<->) ---")
    print("Sintaxis: SELECT campos FROM tabla WHERE campo_sim <-> 'archivo' [METHOD método] LIMIT k;")
    print("Métodos: sequential, inverted")
    print()
    
    # Mostrar tablas multimedia disponibles
    multimedia_tables = parser.list_multimedia_tables()
    if multimedia_tables:
        print("Tablas multimedia disponibles:")
        for table_info in multimedia_tables:
            print(f"  - {table_info['table_name']} ({table_info['media_type']} - {table_info['feature_method']})")
    else:
        print("Warning: No hay tablas multimedia. Crea una primero.")
        return
    
    print("\nEjemplos:")
    print('SELECT * FROM fashion WHERE image_sim <-> "D:/test_image.jpg" LIMIT 8;')
    print('SELECT filename, similarity FROM fashion WHERE image_sim <-> "/path/query.jpg" METHOD inverted LIMIT 5;')
    print()
    
    sql_query = input("Ingresa la consulta SQL: ").strip()
    if sql_query:
        try:
            results = parser.parse_and_execute(sql_query)
            
            if isinstance(results, dict) and 'results' in results:
                # Resultado estructurado de multimedia
                print(f"\n RESULTADOS MULTIMEDIA")
                print("=" * 50)
                print(f" Tiempo de ejecución: {results['execution_time']:.4f} segundos")
                print(f" Resultados encontrados: {results['total_found']}")
                print()
                
                for i, result in enumerate(results['results'][:10], 1):
                    filename = result['filename']
                    similarity = result['similarity']
                    print(f"{i:2d}. [{similarity:.4f}] {filename}")
                    
                    # Mostrar metadatos relevantes si existen
                    metadata = result.get('metadata', {})
                    if metadata:
                        title = metadata.get('productDisplayName', metadata.get('title', ''))
                        if title:
                            print(f"      {title}")
                
                if results['total_found'] > 10:
                    print(f"... y {results['total_found'] - 10} más")
                    
            else:
                print("Warning: Formato de resultados no reconocido")
                
        except Exception as e:
            print(f"Error: {e}")

def show_all_tables_sql(parser):
    """Mostrar información de todas las tablas"""
    print("\n INFORMACIÓN DE TODAS LAS TABLAS")
    print("=" * 60)
    
    # Tablas tradicionales
    traditional_info = parser.engine.list_all_tables_info()
    if traditional_info:
        print("\n TABLAS TRADICIONALES:")
        for table_name, info in traditional_info.items():
            if table_name not in parser.engine.text_tables:
                print(f"   {table_name}: {info.get('index_type', 'N/A')} ({info.get('headers_count', 0)} columnas)")
    
    # Tablas textuales
    text_tables = list(parser.engine.text_tables.keys())
    if text_tables:
        print("\n TABLAS TEXTUALES:")
        for table_name in text_tables:
            info = parser.engine.text_tables[table_name]
            print(f"   {table_name}: SPIMI ({', '.join(info['text_fields'])})")
    
    # Tablas multimedia
    multimedia_tables = parser.list_multimedia_tables()
    if multimedia_tables:
        print("\n TABLAS MULTIMEDIA:")
        for table_info in multimedia_tables:
            name = table_info['table_name']
            media = table_info['media_type']
            method = table_info['feature_method']
            clusters = table_info['n_clusters']
            features = table_info['features_extracted']
            print(f"   {name}: {media}-{method} ({clusters} clusters, {features} características)")
    
    if not traditional_info and not text_tables and not multimedia_tables:
        print("Warning: No hay tablas creadas")

def show_sql_syntax_examples():
    """Mostrar ejemplos de sintaxis SQL"""
    print("\n EJEMPLOS DE SINTAXIS SQL MULTIMEDIA")
    print("=" * 60)
    
    print("\n CREAR TABLA TRADICIONAL:")
    print('CREATE TABLE productos FROM FILE "datos/productos.csv" USING INDEX hash("id");')
    print('CREATE TABLE spatial FROM FILE "datos/coordenadas.csv" USING INDEX rtree("coords");')
    
    print("\n CREAR TABLA TEXTUAL:")
    print('CREATE TABLE musica FROM FILE "datos/songs.csv" USING INDEX spimi("lyrics", "title");')
    print('CREATE TABLE libros FROM FILE "datos/books.csv" USING INDEX text("content", "description");')
    
    print("\n CREAR TABLA MULTIMEDIA:")
    print('CREATE MULTIMEDIA TABLE fashion FROM FILE "datos/fashion.csv" USING image WITH METHOD sift CLUSTERS 128;')
    print('CREATE MULTIMEDIA TABLE audio_db FROM FILE "datos/songs.csv" USING audio WITH METHOD mfcc CLUSTERS 64;')
    
    print("\n BÚSQUEDA TEXTUAL:")
    print('SELECT * FROM musica WHERE lyrics @@ "love and peace" LIMIT 10;')
    print('SELECT title, artist FROM musica WHERE combined_text @@ "rock metal" LIMIT 5;')
    
    print("\n BÚSQUEDA MULTIMEDIA:")
    print('SELECT * FROM fashion WHERE image_sim <-> "D:/query.jpg" LIMIT 8;')
    print('SELECT filename, similarity FROM fashion WHERE image_sim <-> "/path/test.jpg" METHOD inverted LIMIT 5;')
    print('SELECT id, title FROM audio_db WHERE audio_sim <-> "query.wav" METHOD sequential LIMIT 10;')
    
    print("\n OPERACIONES BÁSICAS:")
    print('SELECT * FROM productos;')
    print('INSERT INTO productos VALUES ("1", "Laptop", "Electronics", "999.99");')
    print('DELETE FROM productos WHERE id = "1";')

def interactive_sql_mode(parser):
    """Modo interactivo SQL"""
    print("\n  MODO INTERACTIVO SQL")
    print("=" * 40)
    print("Ingresa consultas SQL. Escribe 'exit' para salir.")
    print("Comandos especiales:")
    print("  help    - Mostrar ayuda")
    print("  tables  - Listar tablas")
    print("  clear   - Limpiar pantalla")
    print()
    
    while True:
        try:
            query = input("SQL> ").strip()
            
            if not query:
                continue
            elif query.lower() == 'exit':
                break
            elif query.lower() == 'help':
                show_sql_syntax_examples()
            elif query.lower() == 'tables':
                show_all_tables_sql(parser)
            elif query.lower() == 'clear':
                os.system('cls' if os.name == 'nt' else 'clear')
            else:
                # Ejecutar consulta SQL
                print()
                start_time = time.time()
                
                try:
                    result = parser.parse_and_execute(query)
                    execution_time = time.time() - start_time
                    
                    if isinstance(result, list):
                        print(f" {len(result)} resultados en {execution_time:.4f}s:")
                        for i, row in enumerate(result[:10], 1):
                            print(f"{i}. {row}")
                        if len(result) > 10:
                            print(f"... y {len(result) - 10} más")
                    elif isinstance(result, dict) and 'results' in result:
                        # Resultado multimedia
                        multimedia_results = result['results']
                        print(f" {len(multimedia_results)} resultados multimedia en {result['execution_time']:.4f}s:")
                        for i, res in enumerate(multimedia_results[:5], 1):
                            print(f"{i}. [{res['similarity']:.4f}] {res['filename']}")
                    elif isinstance(result, str):
                        print(f" {result} ({execution_time:.4f}s)")
                    else:
                        print(f" Operación completada ({execution_time:.4f}s)")
                        print(f"Resultado: {result}")
                
                except Exception as e:
                    print(f"Error: {e}")
                
                print()
                
        except KeyboardInterrupt:
            print("\n Saliendo del modo interactivo...")
            break
        except EOFError:
            break

def show_available_files():
    """Muestra archivos disponibles en el directorio datos"""
    data_dir = 'datos'
    if os.path.exists(data_dir):
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        if files:
            print("Archivos disponibles:")
            for i, file in enumerate(files, 1):
                print(f"  {i}. {file}")
            print()

# ========================================
# FUNCIONES TRADICIONALES
# ========================================

def handle_traditional_engine():
    """Maneja operaciones con el motor tradicional"""
    engine = Engine()
    
    while True:
        try:
            print_traditional_menu()
            choice = input("Selecciona una opción: ").strip()
            
            if choice == '0':
                break
            elif choice == '1':
                load_csv_menu(engine)
            elif choice == '2':
                create_text_index_menu(engine)
            elif choice == '3':
                insert_record_menu(engine)
            elif choice == '4':
                search_menu(engine)
            elif choice == '5':
                range_search_menu(engine)
            elif choice == '6':
                text_search_menu(engine)
            elif choice == '7':
                remove_record_menu(engine)
            elif choice == '8':
                scan_table_menu(engine)
            elif choice == '9':
                show_tables_info(engine)
            else:
                print("Error: Opción inválida")
                
        except KeyboardInterrupt:
            print("\n Volviendo al menú principal...")
            break
        except Exception as e:
            print(f"Error: {e}")

def insert_record_menu(engine: Engine):
    """Insertar registro"""
    tables = list(engine.tables.keys())
    if not tables:
        print("Error: No hay tablas cargadas")
        return
    
    print("Tablas disponibles:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    table_choice = input("Selecciona tabla: ").strip()
    if table_choice.isdigit():
        table_idx = int(table_choice) - 1
        if 0 <= table_idx < len(tables):
            table_name = tables[table_idx]
        else:
            print("Error: Tabla inválida")
            return
    else:
        table_name = table_choice
    
    if table_name not in engine.tables:
        print("Error: Tabla no encontrada")
        return
    
    values_input = input("Valores (separados por coma): ").strip()
    values = [v.strip() for v in values_input.split(',')]
    
    try:
        result = engine.insert(table_name, values)
        print(f" {result}")
    except Exception as e:
        print(f"Error: Error insertando: {e}")

def search_menu(engine: Engine):
    """Buscar por clave"""
    tables = list(engine.tables.keys())
    if not tables:
        print("Error: No hay tablas cargadas")
        return
    
    print("Tablas disponibles:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    table_choice = input("Selecciona tabla: ").strip()
    if table_choice.isdigit():
        table_idx = int(table_choice) - 1
        if 0 <= table_idx < len(tables):
            table_name = tables[table_idx]
        else:
            print("Error: Tabla inválida")
            return
    else:
        table_name = table_choice
    
    key = input("Clave de búsqueda: ").strip()
    column = input("Columna (número): ").strip()
    
    if not column.isdigit():
        print("Error: Columna inválida")
        return
    
    try:
        results = engine.search(table_name, key, int(column))
        print(f"\n Resultados encontrados: {len(results)}")
        for i, result in enumerate(results, 1):
            print(f"{i}. {result}")
    except Exception as e:
        print(f"Error: Error en búsqueda: {e}")

def range_search_menu(engine: Engine):
    """Búsqueda por rango"""
    tables = list(engine.tables.keys())
    if not tables:
        print("Error: No hay tablas cargadas")
        return
    
    print("Tablas disponibles:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    table_choice = input("Selecciona tabla: ").strip()
    if table_choice.isdigit():
        table_idx = int(table_choice) - 1
        if 0 <= table_idx < len(tables):
            table_name = tables[table_idx]
        else:
            print("Error: Tabla inválida")
            return
    else:
        table_name = table_choice
    
    begin_key = input("Clave inicial: ").strip()
    end_key = input("Clave final: ").strip()
    
    try:
        results = engine.range_search(table_name, begin_key, end_key)
        print(f"\n Resultados encontrados: {len(results)}")
        for i, result in enumerate(results, 1):
            print(f"{i}. {result}")
    except Exception as e:
        print(f"Error: Error en búsqueda por rango: {e}")

def text_search_menu(engine: Engine):
    """Búsqueda textual"""
    text_tables = list(engine.text_tables.keys())
    if not text_tables:
        print("Error: No hay tablas con índice textual")
        return
    
    print("Tablas con índice textual:")
    for i, table in enumerate(text_tables, 1):
        print(f"{i}. {table}")
    
    table_choice = input("Selecciona tabla: ").strip()
    if table_choice.isdigit():
        table_idx = int(table_choice) - 1
        if 0 <= table_idx < len(text_tables):
            table_name = text_tables[table_idx]
        else:
            print("Error: Tabla inválida")
            return
    else:
        table_name = table_choice
    
    query = input("Consulta de búsqueda: ").strip()
    if not query:
        print("Error: Consulta vacía")
        return
    
    k = input("Número de resultados (default 10): ").strip()
    k = int(k) if k.isdigit() else 10
    
    try:
        results = engine.textual_search(table_name, query, k)
        print(f"\n Resultados encontrados: {len(results)}")
        for i, (doc, score) in enumerate(results, 1):
            print(f"{i}. Score: {score:.4f}")
            print(f"   {doc}")
            print()
    except Exception as e:
        print(f"Error: Error en búsqueda textual: {e}")

def remove_record_menu(engine: Engine):
    """Eliminar registro"""
    tables = list(engine.tables.keys())
    if not tables:
        print("Error: No hay tablas cargadas")
        return
    
    print("Tablas disponibles:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    table_choice = input("Selecciona tabla: ").strip()
    if table_choice.isdigit():
        table_idx = int(table_choice) - 1
        if 0 <= table_idx < len(tables):
            table_name = tables[table_idx]
        else:
            print("Error: Tabla inválida")
            return
    else:
        table_name = table_choice
    
    key = input("Clave del registro a eliminar: ").strip()
    
    try:
        results = engine.remove(table_name, key)
        print(f" Registros eliminados: {len(results)}")
        for result in results:
            print(f"   {result}")
    except Exception as e:
        print(f"Error: Error eliminando: {e}")

def scan_table_menu(engine: Engine):
    """Mostrar toda la tabla"""
    all_tables = list(engine.tables.keys()) + list(engine.text_tables.keys())
    if not all_tables:
        print("Error: No hay tablas cargadas")
        return
    
    print("Tablas disponibles:")
    for i, table in enumerate(all_tables, 1):
        table_type = "Textual" if table in engine.text_tables else "Tradicional"
        print(f"{i}. {table} ({table_type})")
    
    table_choice = input("Selecciona tabla: ").strip()
    if table_choice.isdigit():
        table_idx = int(table_choice) - 1
        if 0 <= table_idx < len(all_tables):
            table_name = all_tables[table_idx]
        else:
            print("Error: Tabla inválida")
            return
    else:
        table_name = table_choice
    
    try:
        if table_name in engine.tables:
            results = engine.scan(table_name)
            print(f"\n Contenido de la tabla '{table_name}':")
            print(results)
        else:
            print("Error: Solo se puede escanear tablas tradicionales")
    except Exception as e:
        print(f"Error: Error escaneando tabla: {e}")

def show_tables_info(engine: Engine):
    """Mostrar información de todas las tablas"""
    info = engine.list_all_tables_info()
    
    if not info:
        print("Error: No hay tablas cargadas")
        return
    
    print("\n INFORMACIÓN DE TABLAS")
    print("=" * 50)
    
    for table_name, table_info in info.items():
        print(f"\n Tabla: {table_name}")
        print(f"   Tipo: {table_info.get('index_type', 'N/A')}")
        print(f"   Columnas: {table_info.get('headers_count', 0)}")
        print(f"   CSV: {table_info.get('csv_path', 'N/A')}")
        
        if 'text_fields' in table_info:
            print(f"   Campos textuales: {table_info['text_fields']}")
        
        if 'field_index' in table_info:
            print(f"   Campo indexado: {table_info['field_index']}")

def load_csv_menu(engine: Engine):
    """Cargar CSV con índice"""
    print("\n--- Cargar CSV con Índice ---")
    
    # Mostrar archivos CSV disponibles
    data_dir = 'datos'
    if os.path.exists(data_dir):
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        if csv_files:
            print("Archivos CSV disponibles:")
            for i, file in enumerate(csv_files, 1):
                print(f"{i}. {file}")
            
            file_choice = input("Selecciona archivo (número) o ingresa ruta: ").strip()
            
            if file_choice.isdigit():
                file_idx = int(file_choice) - 1
                if 0 <= file_idx < len(csv_files):
                    csv_path = os.path.join(data_dir, csv_files[file_idx])
                else:
                    print("Error: Opción inválida")
                    return
            else:
                csv_path = file_choice
        else:
            csv_path = input("Ingresa la ruta del archivo CSV: ").strip()
    else:
        csv_path = input("Ingresa la ruta del archivo CSV: ").strip()
    
    if not os.path.exists(csv_path):
        print("Error: Archivo no encontrado")
        return
    
    table_name = input("Nombre de la tabla: ").strip()
    if not table_name:
        print("Error: Nombre de tabla requerido")
        return
    
    print("\nTipos de índice disponibles:")
    print("1. Sequential")
    print("2. Hash Extensible")
    print("3. B+ Tree")
    print("4. ISAM")
    print("5. R-Tree")
    
    index_choice = input("Selecciona tipo de índice: ").strip()
    index_map = {
        '1': 'sequential',
        '2': 'hash',
        '3': 'bplustree',
        '4': 'isam',
        '5': 'rtree'
    }
    
    if index_choice not in index_map:
        print("Error: Tipo de índice inválido")
        return
    
    index_type = index_map[index_choice]
    
    if index_type != 'rtree':
        index_field = input("Campo para indexar (número de columna, 0-based): ").strip()
        if not index_field.isdigit():
            print("Error: Campo inválido")
            return
        index_field = int(index_field)
    else:
        index_field = 0  # R-Tree maneja múltiples dimensiones
    
    try:
        result = engine.load_csv(table_name, csv_path, index_type, index_field)
        print(f" {result}")
    except Exception as e:
        print(f"Error: Error cargando CSV: {e}")

def create_text_index_menu(engine: Engine):
    """Crear índice textual SPIMI"""
    print("\n--- Crear Índice Textual SPIMI ---")
    
    csv_path = input("Ruta del archivo CSV: ").strip()
    if not os.path.exists(csv_path):
        print("Error: Archivo no encontrado")
        return
    
    table_name = input("Nombre de la tabla: ").strip()
    if not table_name:
        print("Error: Nombre de tabla requerido")
        return
    
    text_fields = input("Campos de texto (separados por coma): ").strip().split(',')
    text_fields = [field.strip() for field in text_fields if field.strip()]
    
    if not text_fields:
        print("Error: Debe especificar al menos un campo de texto")
        return
    
    index_path = f"indices/{table_name}_spimi.pkl"
    
    print(f"Construyendo índice SPIMI para '{table_name}'...")
    print(f"Campos de texto: {text_fields}")
    
    try:
        # Crear directorio si no existe
        os.makedirs("indices", exist_ok=True)
        # Registrar tabla textual (aquí podrías implementar la construcción real del índice SPIMI)
        engine.register_text_table(table_name, index_path, text_fields, csv_path)
        print(f" Índice textual registrado exitosamente")
    except Exception as e:
        print(f"Error: Error creando índice textual: {e}")

# ========================================
# FUNCIONES MULTIMEDIA
# ========================================

def handle_multimedia_engine():
    """Maneja operaciones con el motor multimedia"""
    multimedia_engine: Optional[MultimediaEngine] = None
    
    while True:
        try:
            print_multimedia_menu()
            choice = input("Selecciona una opción: ").strip()
            
            if choice == '0':
                break
            elif choice == '1':
                multimedia_engine = configure_multimedia_menu()
            elif choice == '2':
                if multimedia_engine is None:
                    print("Error: Primero configura el sistema multimedia")
                else:
                    build_multimedia_index_menu(multimedia_engine)
            elif choice == '3':
                if multimedia_engine is None or not multimedia_engine.is_built:
                    print("Error: Primero construye el índice multimedia")
                else:
                    multimedia_search_menu(multimedia_engine)
            elif choice == '4':
                if multimedia_engine is None or not multimedia_engine.is_built:
                    print("Error: Primero construye el índice multimedia")
                else:
                    multimedia_benchmark_menu(multimedia_engine)
            elif choice == '5':
                multimedia_engine = multimedia_example_menu()
            elif choice == '6':
                if multimedia_engine is None:
                    print("Error: Primero configura el sistema multimedia")
                else:
                    show_multimedia_stats(multimedia_engine)
            else:
                print("Error: Opción inválida")
                
        except KeyboardInterrupt:
            print("\n Volviendo al menú principal...")
            break
        except Exception as e:
            print(f"Error: {e}")

def configure_multimedia_menu() -> Optional[MultimediaEngine]:
    """Configurar sistema multimedia"""
    print("\n--- Configuración Multimedia ---")
    print("1. Imágenes (SIFT)")
    print("2. Imágenes (ResNet50)")
    print("3. Imágenes (InceptionV3)")
    print("4. Audio (MFCC)")
    print("5. Audio (Spectrogram)")
    print("6. Audio (Comprehensive)")
    
    choice = input("Selecciona tipo de multimedia: ").strip()
    
    config_map = {
        '1': ('image', 'sift', 256),
        '2': ('image', 'resnet50', 256),
        '3': ('image', 'inception_v3', 256),
        '4': ('audio', 'mfcc', 128),
        '5': ('audio', 'spectrogram', 128),
        '6': ('audio', 'comprehensive', 128)
    }
    
    if choice in config_map:
        media_type, feature_method, n_clusters = config_map[choice]
        
        # Permitir personalizar clusters
        custom_clusters = input(f"Número de clusters (default {n_clusters}): ").strip()
        if custom_clusters.isdigit():
            n_clusters = int(custom_clusters)
        
        try:
            engine = MultimediaEngine(
                media_type=media_type,
                feature_method=feature_method,
                n_clusters=n_clusters
            )
            print(f" Multimedia configurado: {media_type} - {feature_method} - {n_clusters} clusters")
            return engine
        except Exception as e:
            print(f"Error: Error configurando multimedia: {e}")
            return None
    else:
        print("Error: Opción inválida")
        return None

def build_multimedia_index_menu(engine: MultimediaEngine):
    """Construir índice multimedia"""
    print("\n--- Construir Índice Multimedia ---")
    
    # Cargar dataset
    csv_path = input("Ruta del archivo CSV con metadatos: ").strip()
    if not os.path.exists(csv_path):
        print("Error: Archivo no encontrado")
        return
    
    path_column = input("Nombre de la columna con rutas de archivos: ").strip()
    base_path = input("Ruta base de archivos multimedia (opcional): ").strip()
    
    try:
        # Cargar desde CSV
        import pandas as pd
        df = pd.read_csv(csv_path)
        
        if path_column not in df.columns:
            print(f"Error: Columna '{path_column}' no encontrada")
            print(f"Columnas disponibles: {list(df.columns)}")
            return
        
        print("=== Construyendo Sistema Multimedia ===")
        
        # 1. Extraer características
        print("\n1. Extrayendo características...")
        features_data = engine.extract_features_from_dataframe(
            df=df,
            path_column=path_column,
            base_path=base_path,
            save_features=True,
            features_path=f'multimedia_data/{engine.media_type}_features.pkl'
        )
        
        if not features_data:
            print("Error: No se pudieron extraer características")
            return
        
        # 2. Construir codebook
        print("\n2. Construyendo diccionario...")
        engine.build_codebook(
            save_codebook=True,
            codebook_path=f'multimedia_data/{engine.media_type}_codebook.pkl'
        )
        
        # 3. Crear histogramas
        print("\n3. Creando histogramas...")
        engine.create_histograms(
            save_histograms=True,
            histograms_path=f'multimedia_data/{engine.media_type}_histograms.pkl'
        )
        
        # 4. Construir índices
        print("\n4. Construyendo índices de búsqueda...")
        engine.build_search_indices()
        
        # 5. Guardar sistema
        print("\n5. Guardando sistema...")
        engine.save_complete_system(
            f'multimedia_data/{engine.media_type}_system'
        )
        
        print("\n Sistema multimedia construido exitosamente!")
        
    except Exception as e:
        print(f"Error: Error construyendo índice: {e}")
        import traceback
        traceback.print_exc()

def multimedia_search_menu(engine: MultimediaEngine):
    """Búsqueda multimedia"""
    print("\n--- Búsqueda Multimedia ---")
    
    query_path = input("Ruta del archivo de consulta: ").strip()
    if not os.path.exists(query_path):
        print("Error: Archivo no encontrado")
        return
    
    k = input("Número de resultados (default 10): ").strip()
    k = int(k) if k.isdigit() else 10
    
    print("\nMétodo de búsqueda:")
    print("1. KNN Secuencial")
    print("2. KNN Índice Invertido")
    
    method_choice = input("Selecciona método (default 2): ").strip()
    method = 'sequential' if method_choice == '1' else 'inverted'
    
    try:
        print(f"\nBuscando archivos similares a: {query_path}")
        results = engine.search_similar(query_path, k, method)
        
        print(f"\n=== Resultados de Búsqueda ({method.upper()}) ===")
        print(f"Resultados encontrados: {len(results)}")
        print()
        
        for i, (file_path, similarity) in enumerate(results, 1):
            print(f"{i:2d}. {os.path.basename(file_path)} (similitud: {similarity:.4f})")
            
    except Exception as e:
        print(f"Error: Error en búsqueda: {e}")

def multimedia_benchmark_menu(engine: MultimediaEngine):
    """Benchmark multimedia"""
    print("\n--- Benchmark Multimedia ---")
    
    query_path = input("Ruta del archivo de consulta: ").strip()
    if not os.path.exists(query_path):
        print("Error: Archivo no encontrado")
        return
    
    k = input("Número de resultados (default 8): ").strip()
    k = int(k) if k.isdigit() else 8
    
    try:
        print(f"\n=== Benchmark Multimedia ===")
        print(f"Archivo de consulta: {query_path}")
        print(f"K = {k}")
        
        benchmark_results = engine.benchmark_search_methods(query_path, k=k)
        
        print(f"\nResultados:")
        print(f"KNN Secuencial:       {benchmark_results['sequential']['time']:.4f} segundos")
        print(f"KNN Índice Invertido: {benchmark_results['inverted']['time']:.4f} segundos")
        print(f"Speedup:              {benchmark_results['speedup']:.2f}x")
        
    except Exception as e:
        print(f"Error: Error en benchmark: {e}")

def multimedia_example_menu() -> Optional[MultimediaEngine]:
    """Ejemplo completo multimedia"""
    print("\n--- Ejemplo Completo Multimedia ---")
    print("Este ejemplo usa datos sintéticos para demostrar el funcionamiento")
    
    try:
        # Configurar para imágenes con SIFT
        engine = MultimediaEngine('image', 'sift', 64)  # Pocos clusters para ejemplo
        
        # Crear datos sintéticos
        import numpy as np
        np.random.seed(42)
        
        features_data = []
        for i in range(20):
            # Simular características SIFT
            features = np.random.rand(10, 128).astype(np.float32)
            file_path = f"synthetic_image_{i}.jpg"
            features_data.append((file_path, features))
        
        print("1. Datos sintéticos creados")
        
        # Construir sistema
        engine.features_data = features_data
        engine.build_codebook()
        engine.create_histograms()
        engine.build_search_indices()
        
        print("2. Sistema construido")
        
        # Realizar búsqueda
        query_histogram = engine.histograms_data[0][1]
        results = engine.knn_inverted.search(query_histogram, k=5)
        
        print("3. Búsqueda completada")
        print("\nResultados:")
        for i, (file_path, similarity) in enumerate(results, 1):
            print(f"{i}. {file_path} (similitud: {similarity:.4f})")
        
        # Benchmark
        benchmark = engine.benchmark_search_methods("synthetic_image_0.jpg", k=5)
        print(f"\nBenchmark:")
        print(f"Secuencial: {benchmark['sequential']['time']:.4f}s")
        print(f"Invertido:  {benchmark['inverted']['time']:.4f}s")
        print(f"Speedup:    {benchmark['speedup']:.2f}x")
        
        return engine
        
    except Exception as e:
        print(f"Error: Error en ejemplo: {e}")
        import traceback
        traceback.print_exc()
        return None

def show_multimedia_stats(engine: MultimediaEngine):
    """Mostrar estadísticas multimedia"""
    stats = engine.get_system_statistics()
    
    print("\n ESTADÍSTICAS MULTIMEDIA")
    print("=" * 40)
    print(f"Tipo de media: {stats.get('media_type', 'N/A')}")
    print(f"Método de características: {stats.get('feature_method', 'N/A')}")
    print(f"Número de clusters: {stats.get('n_clusters', 'N/A')}")
    print(f"Características extraídas: {stats.get('features_extracted', 0)}")
    print(f"Histogramas creados: {stats.get('histograms_created', 0)}")
    print(f"Sistema construido: {'' if stats.get('is_built', False) else 'Error:'}")
    
    if stats.get('is_built', False):
        seq_stats = stats.get('sequential_search', {})
        inv_stats = stats.get('inverted_search', {})
        
        print(f"\nÍndice Secuencial:")
        print(f"  Objetos: {seq_stats.get('num_objects', 0)}")
        print(f"  Dimensión: {seq_stats.get('histogram_dimension', 0)}")
        
        print(f"\nÍndice Invertido:")
        print(f"  Documentos: {inv_stats.get('num_documents', 0)}")
        print(f"  Términos: {inv_stats.get('terms_in_index', 0)}")
        print(f"  Compresión: {inv_stats.get('compression_ratio', 0):.2%}")

# ========================================
# FUNCIÓN PRINCIPAL
# ========================================

def show_system_info():
    """Mostrar información general del sistema"""
    print("\n INFORMACIÓN DEL SISTEMA")
    print("=" * 50)
    print("Este sistema soporta tres tipos de operaciones:")
    print()
    print(" ÍNDICES TRADICIONALES:")
    print("  - Sequential File")
    print("  - Hash Extensible")
    print("  - B+ Tree")
    print("  - ISAM")
    print("  - R-Tree")
    print("  - Índices textuales SPIMI")
    print()
    print(" BÚSQUEDA MULTIMEDIA:")
    print("  - Características de imágenes (SIFT, ResNet50, InceptionV3)")
    print("  - Características de audio (MFCC, Spectrogram)")
    print("  - Búsqueda por similitud con TF-IDF")
    print("  - KNN Secuencial y con Índice Invertido")
    print()
    print(" PARSER SQL MULTIMEDIA:")
    print("  - CREATE TABLE tradicional")
    print("  - CREATE TABLE textual con SPIMI")
    print("  - CREATE MULTIMEDIA TABLE")
    print("  - SELECT con operador @@ (textual)")
    print("  - SELECT con operador <-> (multimedia)")
    print("  - Modo interactivo SQL")
    print()
    print(" Directorios del sistema:")
    print(f"  - datos/: {os.path.exists('datos')}")
    print(f"  - multimedia_data/: {os.path.exists('multimedia_data')}")
    print(f"  - indices/: {os.path.exists('indices')}")

def main():
    """Función principal"""
    # Crear directorios necesarios
    os.makedirs('datos', exist_ok=True)
    os.makedirs('multimedia_data', exist_ok=True)
    os.makedirs('indices', exist_ok=True)
    
    print(" Sistema de Base de Datos Multimodal")
    print("Proyecto 2 - BDII - Arquitectura de Motores Separados con Parser SQL")
    
    while True:
        try:
            print_main_menu()
            choice = input("Selecciona una opción: ").strip()
            
            if choice == '1':
                handle_traditional_engine()
            elif choice == '2':
                handle_multimedia_engine()
            elif choice == '3':
                handle_sql_parser()
            elif choice == '4':
                show_system_info()
            elif choice == '5':
                print(" ¡Hasta luego!")
                break
            else:
                print("Error: Opción inválida")
                
        except KeyboardInterrupt:
            print("\n\n ¡Hasta luego!")
            break
        except Exception as e:
            print(f"\nError: Error inesperado: {e}")
            print("Continuando...")

if __name__ == "__main__":
    main()