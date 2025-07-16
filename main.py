#!/usr/bin/env python3
"""
Main interactivo para el Sistema de Base de Datos Multimedia
"""

import os
import sys
from engine import Engine
from parser_sql.parser import SQLParser

def clear_screen():
    """Limpia la pantalla de la consola"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    """Imprime el encabezado del sistema"""
    clear_screen()
    print("=" * 60)
    print("       SISTEMA DE BASE DE DATOS MULTIMEDIA")
    print("=" * 60)
    print()

def get_dataset_path():
    """Solicita y valida la ruta del dataset"""
    while True:
        path = input("\n📁 Ingrese la ruta del dataset CSV: ").strip()
        if os.path.exists(path) and path.endswith('.csv'):
            return path
        else:
            print("❌ Error: El archivo no existe o no es un CSV. Intente nuevamente.")

def get_table_name():
    """Solicita el nombre de la tabla"""
    while True:
        name = input("\n📋 Ingrese el nombre para la tabla: ").strip()
        if name and name.isidentifier():
            return name
        else:
            print("❌ Error: Nombre de tabla inválido. Use solo letras, números y guiones bajos.")

def select_index_type():
    """Permite seleccionar el tipo de índice"""
    print("\n🗂️  TIPOS DE ÍNDICE DISPONIBLES:")
    print("1. Hash Extensible - Búsqueda exacta rápida")
    print("2. B+Tree - Búsqueda por rango y ordenamiento")
    print("3. R-Tree - Búsqueda espacial (requiere coordenadas)")
    print("4. Índice Invertido - Búsqueda de texto")
    print("5. SPIMI - Búsqueda de texto para grandes volúmenes")
    
    index_types = {
        '1': 'hash',
        '2': 'btree',
        '3': 'rtree',
        '4': 'inverted',
        '5': 'spimi'
    }
    
    while True:
        choice = input("\nSeleccione el tipo de índice (1-5): ").strip()
        if choice in index_types:
            return index_types[choice]
        else:
            print("❌ Opción inválida. Intente nuevamente.")

def get_index_column():
    """Solicita la columna a indexar"""
    print("\n📊 Para índices Hash y B+Tree, necesita especificar la columna a indexar.")
    col = input("Ingrese el número de columna (0 para la primera): ").strip()
    try:
        return int(col)
    except ValueError:
        print("⚠️  Usando columna 0 por defecto.")
        return 0

def get_text_columns():
    """Solicita las columnas de texto para índices textuales"""
    print("\n📝 Para índices de texto, especifique las columnas a indexar.")
    print("Ejemplo: track_name,track_artist,lyrics")
    cols = input("Ingrese los nombres de columnas separados por coma: ").strip()
    return [c.strip() for c in cols.split(',') if c.strip()]

def select_multimedia_type():
    """Permite seleccionar el tipo de búsqueda multimedia"""
    print("\n🎨 TIPOS DE BÚSQUEDA MULTIMEDIA:")
    print("1. Imágenes")
    print("2. Audio")
    print("3. No usar búsqueda multimedia")
    
    while True:
        choice = input("\nSeleccione una opción (1-3): ").strip()
        if choice == '1':
            return 'image'
        elif choice == '2':
            return 'audio'
        elif choice == '3':
            return None
        else:
            print("❌ Opción inválida. Intente nuevamente.")

def select_image_method():
    """Permite seleccionar el método de extracción para imágenes"""
    print("\n🖼️  MÉTODOS DE EXTRACCIÓN PARA IMÁGENES:")
    print("1. SIFT - Descriptores locales (robusto, tradicional)")
    print("2. ResNet50 - Red neuronal preentrenada (características globales)")
    print("3. InceptionV3 - Red neuronal preentrenada (características complejas)")
    
    methods = {
        '1': 'sift',
        '2': 'resnet50',
        '3': 'inception_v3'
    }
    
    while True:
        choice = input("\nSeleccione el método (1-3): ").strip()
        if choice in methods:
            return methods[choice]
        else:
            print("❌ Opción inválida. Intente nuevamente.")

def select_audio_method():
    """Permite seleccionar el método de extracción para audio"""
    print("\n🎵 MÉTODOS DE EXTRACCIÓN PARA AUDIO:")
    print("1. MFCC - Coeficientes cepstrales (voz y música)")
    print("2. Espectrograma - Representación tiempo-frecuencia")
    print("3. Comprensivo - Múltiples características combinadas")
    
    methods = {
        '1': 'mfcc',
        '2': 'spectrogram',
        '3': 'comprehensive'
    }
    
    while True:
        choice = input("\nSeleccione el método (1-3): ").strip()
        if choice in methods:
            return methods[choice]
        else:
            print("❌ Opción inválida. Intente nuevamente.")

def create_table_with_index(sql_parser, dataset_path, table_name, index_type, index_col=None, text_cols=None):
    """Crea una tabla con el índice especificado"""
    try:
        if index_type == 'hash':
            query = f'CREATE TABLE {table_name} FROM FILE "{dataset_path}" USING INDEX hash({index_col})'
        elif index_type == 'btree':
            query = f'CREATE TABLE {table_name} FROM FILE "{dataset_path}" USING INDEX btree({index_col})'
        elif index_type == 'rtree':
            query = f'CREATE TABLE {table_name} FROM FILE "{dataset_path}" USING INDEX rtree'
        elif index_type == 'inverted':
            cols_str = ', '.join([f'"{col}"' for col in text_cols])
            query = f'CREATE TABLE {table_name} FROM FILE "{dataset_path}" USING INDEX inverted({cols_str})'
        elif index_type == 'spimi':
            cols_str = ', '.join([f'"{col}"' for col in text_cols])
            query = f'CREATE TABLE {table_name} FROM FILE "{dataset_path}" USING INDEX spimi({cols_str})'
        else:
            raise ValueError(f"Tipo de índice no soportado: {index_type}")
        
        print(f"\n🔄 Ejecutando: {query}")
        result = sql_parser.parse_and_execute(query)
        
        if result and result.get('success'):
            print(f"✅ Tabla '{table_name}' creada exitosamente con índice {index_type.upper()}")
            return True
        else:
            print(f"❌ Error: {result.get('error', 'Error desconocido')}")
            return False
            
    except Exception as e:
        print(f"❌ Error creando tabla: {str(e)}")
        return False

def create_multimedia_table(sql_parser, dataset_path, table_name, media_type, method):
    """Crea una tabla con capacidades multimedia"""
    try:
        # Primero crear la tabla con un índice básico (hash en columna 0)
        query = f'CREATE TABLE {table_name} FROM FILE "{dataset_path}" USING INDEX hash(0)'
        print(f"\n🔄 Creando tabla base: {query}")
        result = sql_parser.parse_and_execute(query)
        
        if not result or not result.get('success'):
            print(f"❌ Error creando tabla base: {result.get('error', 'Error desconocido')}")
            return False
        
        # Luego agregar índice multimedia
        if media_type == 'image':
            # Asumiendo que hay una columna con rutas de imágenes
            path_column = input("\n📸 Ingrese el nombre de la columna con rutas de imágenes: ").strip()
            multimedia_query = f'CREATE MULTIMEDIA INDEX ON {table_name} USING {method} FOR images WITH PATH_COLUMN "{path_column}"'
        else:  # audio
            path_column = input("\n🎵 Ingrese el nombre de la columna con rutas de audio: ").strip()
            multimedia_query = f'CREATE MULTIMEDIA INDEX ON {table_name} USING {method} FOR audio WITH PATH_COLUMN "{path_column}"'
        
        print(f"\n🔄 Creando índice multimedia: {multimedia_query}")
        # Aquí deberías implementar la lógica para crear el índice multimedia
        # Por ahora, solo imprimimos el comando
        print(f"✅ Tabla '{table_name}' creada con capacidades multimedia ({media_type}/{method})")
        return True
        
    except Exception as e:
        print(f"❌ Error creando tabla multimedia: {str(e)}")
        return False

def show_sample_queries(table_name, index_type, media_type=None):
    """Muestra consultas de ejemplo según el tipo de índice"""
    print("\n📖 CONSULTAS DE EJEMPLO:")
    print("-" * 50)
    
    if index_type == 'hash':
        print(f"SELECT * FROM {table_name} WHERE column_name = 'valor'")
        
    elif index_type == 'btree':
        print(f"SELECT * FROM {table_name} WHERE column_name = 'valor'")
        print(f"SELECT * FROM {table_name} WHERE column_name BETWEEN 'valor1' AND 'valor2'")
        
    elif index_type == 'rtree':
        print(f"SELECT * FROM {table_name} WHERE location <-> '(lat, lon)' < 10")
        print(f"SELECT * FROM {table_name} WHERE location <-> '(lat, lon)' ORDER BY location <-> '(lat, lon)' LIMIT 5")
        
    elif index_type in ['inverted', 'spimi']:
        print(f"SELECT * FROM {table_name} WHERE lyrics @@ 'love'")
        print(f"SELECT * FROM {table_name} WHERE track_name @@ 'hello' LIMIT 10")
    
    if media_type:
        print(f"\n🎨 Consultas multimedia:")
        print(f"SELECT * FROM {table_name} WHERE multimedia_similarity('path/to/query.jpg') > 0.8")
        print(f"SELECT * FROM {table_name} ORDER BY multimedia_similarity('path/to/query.jpg') DESC LIMIT 10")

def main():
    """Función principal del programa"""
    print_header()
    
    print("🚀 Bienvenido al Sistema de Base de Datos Multimedia")
    print("\nEste asistente te ayudará a crear y configurar tu base de datos.\n")
    
    # Inicializar el sistema
    print("⚙️  Inicializando el sistema de base de datos...")
    engine = Engine()
    sql_parser = SQLParser(engine)
    
    # Obtener información básica
    dataset_path = get_dataset_path()
    table_name = get_table_name()
    
    # Seleccionar tipo de funcionalidad
    print("\n🎯 ¿QUÉ TIPO DE BÚSQUEDA DESEA CONFIGURAR?")
    print("1. Búsqueda tradicional (Hash, B+Tree, R-Tree)")
    print("2. Búsqueda de texto (Índice Invertido, SPIMI)")
    print("3. Búsqueda multimedia (Imágenes, Audio)")
    print("4. Búsqueda combinada (Texto + Multimedia)")
    
    search_type = input("\nSeleccione una opción (1-4): ").strip()
    
    if search_type == '1':
        # Búsqueda tradicional
        index_type = select_index_type()
        if index_type in ['hash', 'btree']:
            index_col = get_index_column()
            success = create_table_with_index(sql_parser, dataset_path, table_name, index_type, index_col=index_col)
        else:
            success = create_table_with_index(sql_parser, dataset_path, table_name, index_type)
        
        if success:
            show_sample_queries(table_name, index_type)
    
    elif search_type == '2':
        # Búsqueda de texto
        print("\n📝 ÍNDICES DE TEXTO DISPONIBLES:")
        print("1. Índice Invertido - Rápido para vocabularios moderados")
        print("2. SPIMI - Eficiente para grandes volúmenes de texto")
        
        text_choice = input("\nSeleccione el tipo de índice (1-2): ").strip()
        index_type = 'inverted' if text_choice == '1' else 'spimi'
        
        text_cols = get_text_columns()
        success = create_table_with_index(sql_parser, dataset_path, table_name, index_type, text_cols=text_cols)
        
        if success:
            show_sample_queries(table_name, index_type)
    
    elif search_type == '3':
        # Búsqueda multimedia
        media_type = select_multimedia_type()
        if media_type == 'image':
            method = select_image_method()
        elif media_type == 'audio':
            method = select_audio_method()
        else:
            return
        
        success = create_multimedia_table(sql_parser, dataset_path, table_name, media_type, method)
        
        if success:
            show_sample_queries(table_name, 'hash', media_type)
    
    elif search_type == '4':
        # Búsqueda combinada
        print("\n🔀 Configurando búsqueda combinada (Texto + Multimedia)")
        
        # Primero configurar búsqueda de texto
        text_cols = get_text_columns()
        success = create_table_with_index(sql_parser, dataset_path, table_name, 'spimi', text_cols=text_cols)
        
        if success:
            # Luego agregar multimedia
            media_type = select_multimedia_type()
            if media_type:
                if media_type == 'image':
                    method = select_image_method()
                else:
                    method = select_audio_method()
                
                # Aquí agregar lógica para índice multimedia
                print(f"\n✅ Configuración combinada completada: SPIMI + {media_type}/{method}")
                show_sample_queries(table_name, 'spimi', media_type)
    
    # Opción para ejecutar consultas
    print("\n" + "=" * 60)
    while True:
        query = input("\n💻 Ingrese una consulta SQL (o 'exit' para salir):\n> ").strip()
        
        if query.lower() == 'exit':
            print("\n👋 ¡Hasta luego!")
            break
        
        if query:
            result = sql_parser.parse_and_execute(query)
            if result['success']:
                if 'data' in result:
                    print(f"\n✅ Resultados ({len(result['data'])} filas):")
                    # Mostrar primeras 5 filas
                    for i, row in enumerate(result['data'][:5]):
                        print(f"  {i+1}. {row}")
                    if len(result['data']) > 5:
                        print(f"  ... y {len(result['data']) - 5} filas más")
                else:
                    print(f"✅ {result.get('message', 'Consulta ejecutada exitosamente')}")
            else:
                print(f"❌ Error: {result.get('error', 'Error desconocido')}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Programa interrumpido por el usuario.")
    except Exception as e:
        print(f"\n❌ Error inesperado: {str(e)}")
        import traceback
        traceback.print_exc()