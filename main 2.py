#!/usr/bin/env python3
"""
Sistema de Base de Datos Multimodal - Proyecto 2
Testing simplificado para índices textuales SPIMI
"""

import sys
import os
import time
from typing import List, Dict, Any
from engine import Engine
from parser_sql.parser import SQLParser

def main():
    """Función principal para testing de SPIMI"""
    print("🎵 SISTEMA DE BASE DE DATOS MULTIMODAL - TESTING SPIMI")
    print("="*60)
    
    # Verificar archivo de datos
    csv_file = "datos/spotify_songs.csv"
    if not os.path.exists(csv_file):
        print(f"❌ Archivo no encontrado: {csv_file}")
        print("📁 Asegúrate de que el archivo existe en la ruta especificada")
        return
    
    # Inicializar sistema
    engine = Engine()
    parser = SQLParser(engine)
    
    print(f"📁 Archivo encontrado: {csv_file}")
    print("🔧 Inicializando sistema...\n")
    
    # ============ PASO 1: CREAR TABLA CON ÍNDICE SPIMI ============
    print("🔨 PASO 1: CREANDO TABLA CON ÍNDICE SPIMI")
    print("-" * 50)
    
    create_query = '''CREATE TABLE Spotify
FROM FILE "datos/spotify_songs.csv"
USING INDEX SPIMI ("track_name", "track_artist", "track_album_name", "lyrics");'''
    
    print(f"📝 Ejecutando: {create_query}")
    
    try:
        start_time = time.time()
        result = parser.parse_and_execute(create_query)
        creation_time = time.time() - start_time
        
        print(f"✅ {result}")
        print(f"⏱️ Tiempo de creación: {creation_time:.2f} segundos\n")
        
    except Exception as e:
        print(f"❌ Error creando tabla: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ============ PASO 2: EJECUTAR CONSULTAS DE TESTING ============
    print("🔍 PASO 2: EJECUTANDO CONSULTAS TEXTUALES")
    print("-" * 50)
    
    # Consultas de testing según especificaciones del usuario
    test_queries = [
        # Consulta 1: Búsqueda en lyrics con palabras en inglés
        {
            'description': 'Búsqueda en lyrics: "love heart"',
            'query': '''SELECT * FROM Spotify WHERE lyrics @@ 'love heart' LIMIT 5;'''
        },
        
        # Consulta 2: Campos específicos con palabras en español
        {
            'description': 'Campos específicos con palabras en español',
            'query': '''SELECT track_name, track_artist FROM Spotify WHERE lyrics @@ 'amor corazón' LIMIT 10;'''
        },
        
        # Consulta 3: Búsqueda en nombre de canción
        {
            'description': 'Búsqueda en track_name: "freedom"',
            'query': '''SELECT * FROM Spotify WHERE track_name @@ 'freedom' LIMIT 3;'''
        },
        
        # Consulta 4: Frase entre comillas dobles
        {
            'description': 'Frase entre comillas: "dancing in the dark"',
            'query': '''SELECT * FROM Spotify WHERE lyrics @@ "dancing in the dark" LIMIT 5;'''
        }
    ]
    
    # Ejecutar cada consulta
    for i, test_case in enumerate(test_queries, 1):
        print(f"\n--- TEST {i}: {test_case['description']} ---")
        print(f"📝 Query: {test_case['query']}")
        
        try:
            start_time = time.time()
            results = parser.parse_and_execute(test_case['query'])
            query_time = time.time() - start_time
            
            if results and len(results) > 0:
                print(f"✅ Encontrados {len(results)} resultados en {query_time:.4f}s")
                
                # Mostrar primeros resultados
                for j, result in enumerate(results[:3], 1):
                    # Extraer información básica del CSV
                    try:
                        import csv
                        import io
                        reader = csv.reader(io.StringIO(result))
                        values = next(reader, [])
                        
                        # Asumir que las primeras columnas son: track_id, track_name, track_artist
                        track_name = values[1] if len(values) > 1 else "N/A"
                        track_artist = values[2] if len(values) > 2 else "N/A" 
                        score = values[-1] if len(values) > 0 else "0.0"  # Score siempre es el último
                        
                        print(f"  {j}. [{score}] {track_name} - {track_artist}")
                        
                    except Exception as e:
                        print(f"  {j}. {result[:100]}...")
                
                if len(results) > 3:
                    print(f"  ... y {len(results) - 3} resultados más")
            else:
                print(f"⚠️ No se encontraron resultados ({query_time:.4f}s)")
                
        except Exception as e:
            print(f"❌ Error ejecutando consulta: {e}")
            import traceback
            traceback.print_exc()
        
        # Pausa entre consultas
        if i < len(test_queries):
            input("\n🔵 Presiona Enter para continuar con la siguiente consulta...")
    
    # ============ PASO 3: INFORMACIÓN DEL SISTEMA ============
    print(f"\n🔍 PASO 3: INFORMACIÓN DEL SISTEMA")
    print("-" * 50)
    
    try:
        # Mostrar información de las tablas
        tables_info = engine.list_all_tables_info()
        
        for table_name, info in tables_info.items():
            print(f"\n📊 Tabla: {table_name}")
            print(f"   Tipo de índice: {info.get('index_type', 'N/A')}")
            print(f"   Número de columnas: {info.get('headers_count', 0)}")
            
            if 'text_fields' in info:
                print(f"   Campos textuales: {', '.join(info['text_fields'])}")
            
            if 'csv_path' in info:
                try:
                    # Contar registros en el CSV
                    with open(info['csv_path'], 'r', encoding='utf-8') as f:
                        line_count = sum(1 for line in f) - 1  # -1 por el header
                    print(f"   Registros en CSV: {line_count:,}")
                except:
                    pass
                    
    except Exception as e:
        print(f"❌ Error obteniendo información del sistema: {e}")
    
    print(f"\n✅ TESTING COMPLETADO")
    print("="*60)

def interactive_mode():
    """Modo interactivo simple para testing adicional"""
    print("\n🎮 MODO INTERACTIVO")
    print("-" * 30)
    print("💡 Escribe consultas SQL con operador @@")
    print("📝 Ejemplo: SELECT * FROM Spotify WHERE lyrics @@ 'love' LIMIT 3;")
    print("🚪 Escribe 'exit' para salir\n")
    
    engine = Engine()
    parser = SQLParser(engine)
    
    # Verificar si ya existe la tabla
    csv_file = "datos/spotify_songs.csv"
    if os.path.exists(csv_file):
        try:
            create_query = '''CREATE TABLE Spotify FROM FILE "datos/spotify_songs.csv" USING INDEX SPIMI ("track_name", "track_artist", "lyrics");'''
            parser.parse_and_execute(create_query)
            print("✅ Tabla Spotify cargada\n")
        except Exception as e:
            print(f"❌ Error cargando tabla: {e}\n")
            return
    
    while True:
        try:
            query = input("🎵 SQL> ").strip()
            
            if query.lower() == 'exit':
                print("👋 ¡Hasta luego!")
                break
            
            if not query:
                continue
            
            if ' @@ ' not in query:
                print("⚠️ Use consultas con operador @@ para búsqueda textual")
                continue
                
            start_time = time.time()
            results = parser.parse_and_execute(query)
            query_time = time.time() - start_time
            
            if results:
                print(f"✅ {len(results)} resultados en {query_time:.4f}s")
                for i, result in enumerate(results[:5], 1):
                    print(f"  {i}. {result[:100]}...")
                if len(results) > 5:
                    print(f"  ... y {len(results) - 5} más")
            else:
                print(f"⚠️ No se encontraron resultados")
                
        except KeyboardInterrupt:
            print("\n👋 ¡Hasta luego!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        interactive_mode()
    else:
        main()
        
        # Preguntar si quiere modo interactivo
        choice = input("\n🎮 ¿Quieres probar el modo interactivo? (y/n): ").strip().lower()
        if choice in ['y', 'yes', 's', 'si']:
            interactive_mode()