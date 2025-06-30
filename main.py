#!/usr/bin/env python3
"""
Sistema de Base de Datos Multimodal - Proyecto 2
Testing interactivo para índices textuales SPIMI
"""

import sys
import os
import time
from typing import List, Dict, Any
from engine import Engine
from parser_sql.parser import SQLParser

def interactive_sql_mode():
    """Modo SQL interactivo donde el usuario controla todo"""
    print("🎵 SISTEMA DE BASE DE DATOS MULTIMODAL - MODO SQL INTERACTIVO")
    print("="*70)
    print("💡 Escriba consultas SQL completas")
    print("🔧 Ejemplo CREATE TABLE:")
    print('   CREATE TABLE Spotify FROM FILE "datos/spotify_songs.csv"')
    print('   USING INDEX SPIMI ("track_name", "track_artist", "lyrics");')
    print("\n🔍 Ejemplo búsquedas:")
    print("   SELECT * FROM Spotify WHERE lyrics @@ 'love heart' LIMIT 5;")
    print("   SELECT track_name, track_artist FROM Spotify WHERE lyrics @@ 'amor' LIMIT 3;")
    print("\n🎮 Comandos: 'help', 'tables', 'exit'")
    print("="*70)
    
    # Inicializar sistema
    engine = Engine()
    parser = SQLParser(engine)
    
    while True:
        try:
            query = input("\n🎵 SQL> ").strip()
            
            if query.lower() == 'exit':
                print("👋 ¡Hasta luego!")
                break
            elif query.lower() == 'help':
                print("\n📖 COMANDOS DISPONIBLES:")
                print("  CREATE TABLE - Crear tabla con índice")
                print("  SELECT con @@ - Búsqueda textual")
                print("  tables - Ver tablas cargadas") 
                print("  help - Mostrar esta ayuda")
                print("  exit - Salir")
                print("\n📝 SINTAXIS:")
                print('  CREATE TABLE nombre FROM FILE "ruta.csv" USING INDEX SPIMI ("campo1", "campo2");')
                print("  SELECT campos FROM tabla WHERE campo @@ 'consulta' LIMIT k;")
                continue
            elif query.lower() == 'tables':
                print("\n📊 TABLAS CARGADAS:")
                tables_info = engine.list_all_tables_info()
                if not tables_info:
                    print("  ⚠️ No hay tablas cargadas")
                else:
                    for table_name, info in tables_info.items():
                        print(f"  📋 {table_name}: {info.get('index_type', 'N/A')} ({info.get('headers_count', 0)} columnas)")
                        if 'text_fields' in info:
                            print(f"      Campos textuales: {', '.join(info['text_fields'])}")
                continue
            
            if not query:
                continue
            
            # Ejecutar consulta
            print(f"\n⚡ Ejecutando: {query}")
            start_time = time.time()
            
            try:
                results = parser.parse_and_execute(query)
                execution_time = time.time() - start_time
                
                # Manejar diferentes tipos de resultados
                if isinstance(results, str):
                    # CREATE TABLE u otras operaciones que retornan string
                    print(f"✅ {results}")
                    print(f"⏱️ Tiempo: {execution_time:.4f}s")
                    
                elif isinstance(results, list) and len(results) > 0:
                    # SELECT que retorna lista de resultados
                    print(f"✅ Encontrados {len(results)} resultados en {execution_time:.4f}s")
                    print(f"\n📋 RESULTADOS:")
                    print("-" * 60)
                    
                    for i, result in enumerate(results[:10], 1):  # Mostrar máximo 10
                        # Extraer información del CSV
                        try:
                            import csv
                            import io
                            reader = csv.reader(io.StringIO(result))
                            values = next(reader, [])
                            
                            # Detectar si tiene score al final (búsqueda textual)
                            if len(values) > 0:
                                try:
                                    score = float(values[-1])
                                    if 0 <= score <= 1:  # Es probable que sea un score
                                        track_name = values[1] if len(values) > 1 else "N/A"
                                        track_artist = values[2] if len(values) > 2 else "N/A"
                                        print(f"  {i:2d}. [{score:.4f}] {track_name} - {track_artist}")
                                    else:
                                        print(f"  {i:2d}. {result[:100]}{'...' if len(result) > 100 else ''}")
                                except ValueError:
                                    print(f"  {i:2d}. {result[:100]}{'...' if len(result) > 100 else ''}")
                            else:
                                print(f"  {i:2d}. {result[:100]}{'...' if len(result) > 100 else ''}")
                                
                        except Exception:
                            print(f"  {i:2d}. {result[:100]}{'...' if len(result) > 100 else ''}")
                    
                    if len(results) > 10:
                        print(f"  ... y {len(results) - 10} resultados más")
                        
                elif isinstance(results, list) and len(results) == 0:
                    print(f"⚠️ No se encontraron resultados ({execution_time:.4f}s)")
                    
                else:
                    print(f"✅ Operación completada ({execution_time:.4f}s)")
                    
            except Exception as e:
                print(f"❌ Error: {e}")
                # Mostrar error más detallado solo si es útil
                if "no encontrada" in str(e).lower():
                    print("💡 Tip: Primero crea una tabla con CREATE TABLE")
                elif "operador @@" in str(e).lower():
                    print("💡 Tip: Use sintaxis SELECT campos FROM tabla WHERE campo @@ 'consulta' LIMIT k;")
                
        except KeyboardInterrupt:
            print("\n👋 ¡Hasta luego!")
            break
        except EOFError:
            print("\n👋 ¡Hasta luego!")
            break

def demo_mode():
    """Modo demo automático (el comportamiento anterior)"""
    print("🎵 SISTEMA DE BASE DE DATOS MULTIMODAL - MODO DEMO")
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
    
    # Ejecutar demo automático
    print("🔨 CREANDO TABLA CON ÍNDICE SPIMI")
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
        
        # Ejecutar consultas de demo
        demo_queries(parser)
        
    except Exception as e:
        print(f"❌ Error creando tabla: {e}")
        import traceback
        traceback.print_exc()

def demo_queries(parser):
    """Ejecuta consultas de demostración"""
    print("🔍 EJECUTANDO CONSULTAS DE DEMOSTRACIÓN")
    print("-" * 50)
    
    test_queries = [
        {
            'description': 'Búsqueda en lyrics: "love heart"',
            'query': '''SELECT * FROM Spotify WHERE lyrics @@ 'love heart' LIMIT 5;'''
        },
        {
            'description': 'Campos específicos con español',
            'query': '''SELECT track_name, track_artist FROM Spotify WHERE lyrics @@ 'amor corazón' LIMIT 10;'''
        },
        {
            'description': 'Búsqueda en track_name: "freedom"',
            'query': '''SELECT * FROM Spotify WHERE track_name @@ 'freedom' LIMIT 3;'''
        },
        {
            'description': 'Frase entre comillas',
            'query': '''SELECT * FROM Spotify WHERE lyrics @@ "dancing in the dark" LIMIT 5;'''
        }
    ]
    
    for i, test_case in enumerate(test_queries, 1):
        print(f"\n--- DEMO {i}: {test_case['description']} ---")
        print(f"📝 Query: {test_case['query']}")
        
        try:
            start_time = time.time()
            results = parser.parse_and_execute(test_case['query'])
            query_time = time.time() - start_time
            
            if results and len(results) > 0:
                print(f"✅ Encontrados {len(results)} resultados en {query_time:.4f}s")
                
                for j, result in enumerate(results[:3], 1):
                    try:
                        import csv
                        import io
                        reader = csv.reader(io.StringIO(result))
                        values = next(reader, [])
                        
                        track_name = values[1] if len(values) > 1 else "N/A"
                        track_artist = values[2] if len(values) > 2 else "N/A" 
                        score = values[-1] if len(values) > 0 else "0.0"
                        
                        print(f"  {j}. [{score}] {track_name} - {track_artist}")
                        
                    except Exception:
                        print(f"  {j}. {result[:100]}...")
                
                if len(results) > 3:
                    print(f"  ... y {len(results) - 3} resultados más")
            else:
                print(f"⚠️ No se encontraron resultados ({query_time:.4f}s)")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        
        if i < len(test_queries):
            input("\n🔵 Presiona Enter para continuar...")

def main():
    """Función principal con opciones"""
    if len(sys.argv) > 1:
        if sys.argv[1] == '--demo':
            demo_mode()
        elif sys.argv[1] == '--interactive':
            interactive_sql_mode()
        else:
            print("Uso: python main.py [--demo|--interactive]")
    else:
        # Preguntar qué modo quiere
        print("🎵 SISTEMA DE BASE DE DATOS MULTIMODAL")
        print("="*50)
        print("Selecciona el modo:")
        print("1. 🎮 Interactivo (tú escribes las consultas SQL)")
        print("2. 🚀 Demo automático (ejecuta ejemplos)")
        print("3. 🚪 Salir")
        
        while True:
            choice = input("\n🎯 Opción (1-3): ").strip()
            
            if choice == '1':
                interactive_sql_mode()
                break
            elif choice == '2':
                demo_mode()
                break
            elif choice == '3':
                print("👋 ¡Hasta luego!")
                break
            else:
                print("❌ Opción inválida. Selecciona 1, 2 o 3.")

if __name__ == "__main__":
    main()