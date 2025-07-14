#!/usr/bin/env python3
"""
Benchmark script para comparar el índice invertido personalizado con PostgreSQL
para búsqueda de texto usando el dataset de Spotify.

Evalúa rendimiento en diferentes tamaños de datos: 1k, 2k, 4k, 8k, 12k, 16k registros
"""

import os
import sys
import time
import pandas as pd
import psycopg2
import numpy as np
from typing import List, Dict, Tuple, Any
import json
from datetime import datetime

# Importar nuestro índice invertido
sys.path.append('.')
from indices.inverted_index import InvertedIndex

class TextSearchBenchmark:
    """Clase para ejecutar benchmarks de búsqueda de texto"""
    
    def __init__(self):
        # Configuración de PostgreSQL
        self.pg_config = {
            'host': 'localhost',
            'database': 'postgres',
            'user': 'msalazarh',
            'password': '',  # Sin password para conexión local
            'port': 5432
        }
        
        # Campos de texto para búsqueda
        self.text_fields = ['track_name', 'track_artist', 'lyrics', 'track_album_name']
        
        # Tamaños de datasets a probar
        self.dataset_sizes = [1000, 2000, 4000, 8000, 12000, 16000]
        
        # Consultas de prueba
        self.test_queries = [
            "love heart",
            "dance party music",
            "rock guitar solo",
            "sad lonely night",
            "happy celebration",
            "summer sunshine",
            "cold winter",
            "beautiful woman",
            "life dreams",
            "soul music"
        ]
        
        # Resultados
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'benchmarks': []
        }
    
    def setup_postgresql(self):
        """Configura PostgreSQL con extensiones de búsqueda de texto"""
        print("🔧 Configurando PostgreSQL...")
        
        try:
            conn = psycopg2.connect(**self.pg_config)
            cur = conn.cursor()
            
            # Crear extensión de búsqueda de texto si no existe
            cur.execute("CREATE EXTENSION IF NOT EXISTS unaccent;")
            
            # Configurar idioma español para text search
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_ts_config WHERE cfgname = 'spanish_simple') THEN
                        CREATE TEXT SEARCH CONFIGURATION spanish_simple (COPY = spanish);
                        ALTER TEXT SEARCH CONFIGURATION spanish_simple
                            ALTER MAPPING FOR asciiword, asciihword, hword_asciipart, word, hword, hword_part
                            WITH unaccent, spanish_stem;
                    END IF;
                END
                $$;
            """)
            
            conn.commit()
            conn.close()
            print("✅ PostgreSQL configurado correctamente")
            return True
            
        except Exception as e:
            print(f"❌ Error configurando PostgreSQL: {e}")
            return False
    
    def create_postgres_table_and_index(self, data: pd.DataFrame, table_name: str):
        """Crea tabla y índices GIN en PostgreSQL"""
        print(f"📊 Creando tabla PostgreSQL: {table_name}")
        
        try:
            conn = psycopg2.connect(**self.pg_config)
            cur = conn.cursor()
            
            # Eliminar tabla si existe
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            
            # Crear tabla
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    track_name TEXT,
                    track_artist TEXT,
                    lyrics TEXT,
                    track_album_name TEXT,
                    track_popularity INTEGER,
                    playlist_genre TEXT,
                    playlist_subgenre TEXT,
                    search_content tsvector
                );
            """)
            
            # Insertar datos
            print(f"📥 Insertando {len(data)} registros...")
            for idx, row in data.iterrows():
                # Combinar campos de texto para búsqueda
                combined_text = ' '.join([
                    str(row.get('track_name', '')),
                    str(row.get('track_artist', '')),
                    str(row.get('lyrics', '')),
                    str(row.get('track_album_name', ''))
                ])
                
                cur.execute(f"""
                    INSERT INTO {table_name} 
                    (track_name, track_artist, lyrics, track_album_name, 
                     track_popularity, playlist_genre, playlist_subgenre, search_content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, to_tsvector('spanish_simple', %s))
                """, (
                    str(row.get('track_name', '')),
                    str(row.get('track_artist', '')),
                    str(row.get('lyrics', '')),
                    str(row.get('track_album_name', '')),
                    int(row.get('track_popularity', 0)),
                    str(row.get('playlist_genre', '')),
                    str(row.get('playlist_subgenre', '')),
                    combined_text
                ))
            
            # Crear índice GIN para búsqueda de texto
            print("🗂️ Creando índice GIN...")
            cur.execute(f"""
                CREATE INDEX idx_{table_name}_search_gin 
                ON {table_name} USING GIN(search_content);
            """)
            
            # Crear índices adicionales para comparación
            cur.execute(f"""
                CREATE INDEX idx_{table_name}_popularity 
                ON {table_name}(track_popularity DESC);
            """)
            
            # Analizar tabla para optimizar consultas
            cur.execute(f"ANALYZE {table_name};")
            
            conn.commit()
            conn.close()
            print("✅ Tabla PostgreSQL creada con índices")
            return True
            
        except Exception as e:
            print(f"❌ Error creando tabla PostgreSQL: {e}")
            return False
    
    def benchmark_postgresql(self, table_name: str, query: str, k: int = 10) -> Tuple[float, List[Dict]]:
        """Ejecuta benchmark en PostgreSQL usando ts_rank"""
        try:
            conn = psycopg2.connect(**self.pg_config)
            cur = conn.cursor()
            
            # Preparar consulta con ranking
            sql_query = f"""
                SELECT 
                    track_name, track_artist, track_album_name, track_popularity,
                    ts_rank_cd(search_content, plainto_tsquery('spanish_simple', %s)) as rank
                FROM {table_name}
                WHERE search_content @@ plainto_tsquery('spanish_simple', %s)
                ORDER BY rank DESC, track_popularity DESC
                LIMIT %s;
            """
            
            start_time = time.time()
            cur.execute(sql_query, (query, query, k))
            results = cur.fetchall()
            end_time = time.time()
            
            # Convertir resultados a formato compatible
            formatted_results = []
            for row in results:
                formatted_results.append({
                    'track_name': row[0],
                    'track_artist': row[1],
                    'track_album_name': row[2],
                    'track_popularity': row[3],
                    'score': float(row[4])
                })
            
            conn.close()
            return end_time - start_time, formatted_results
            
        except Exception as e:
            print(f"❌ Error en benchmark PostgreSQL: {e}")
            return float('inf'), []
    
    def benchmark_custom_index(self, index: InvertedIndex, query: str, k: int = 10) -> Tuple[float, List[Dict]]:
        """Ejecuta benchmark en nuestro índice invertido"""
        try:
            start_time = time.time()
            results = index.search(query, k)
            end_time = time.time()
            
            # Formatear resultados para compatibilidad
            formatted_results = []
            for doc, score in results:
                formatted_results.append({
                    'track_name': doc.get('track_name', ''),
                    'track_artist': doc.get('track_artist', ''),
                    'track_album_name': doc.get('track_album_name', ''),
                    'track_popularity': doc.get('track_popularity', 0),
                    'score': float(score)
                })
            
            return end_time - start_time, formatted_results
            
        except Exception as e:
            print(f"❌ Error en benchmark índice personalizado: {e}")
            return float('inf'), []
    
    def compare_result_quality(self, pg_results: List[Dict], custom_results: List[Dict]) -> Dict:
        """Compara la calidad de los resultados entre sistemas"""
        
        # Extraer IDs únicos para comparación (usando track_name + artist)
        pg_tracks = set((r['track_name'], r['track_artist']) for r in pg_results)
        custom_tracks = set((r['track_name'], r['track_artist']) for r in custom_results)
        
        # Calcular métricas de similitud
        intersection = len(pg_tracks.intersection(custom_tracks))
        union = len(pg_tracks.union(custom_tracks))
        
        # Jaccard similarity
        jaccard = intersection / union if union > 0 else 0
        
        # Overlap en top-K
        overlap_top5 = len(set(list(pg_tracks)[:5]).intersection(set(list(custom_tracks)[:5])))
        overlap_top10 = len(set(list(pg_tracks)[:10]).intersection(set(list(custom_tracks)[:10])))
        
        return {
            'total_results_pg': len(pg_results),
            'total_results_custom': len(custom_results),
            'intersection_count': intersection,
            'jaccard_similarity': jaccard,
            'overlap_top5': overlap_top5,
            'overlap_top10': overlap_top10
        }
    
    def run_benchmark_for_size(self, data: pd.DataFrame, size: int):
        """Ejecuta benchmark completo para un tamaño específico"""
        print(f"\n🚀 Iniciando benchmark para {size} registros...")
        
        # Tomar muestra del dataset
        sample_data = data.head(size).copy()
        table_name = f"spotify_benchmark_{size}"
        
        # Configurar PostgreSQL
        pg_setup_start = time.time()
        pg_success = self.create_postgres_table_and_index(sample_data, table_name)
        pg_setup_time = time.time() - pg_setup_start
        
        if not pg_success:
            print(f"❌ Falló configuración PostgreSQL para {size} registros")
            return
        
        # Configurar índice personalizado
        custom_setup_start = time.time()
        custom_index = InvertedIndex(f"spotify_{size}", self.text_fields, 'spanish')
        
        # Convertir DataFrame a lista de diccionarios
        data_list = sample_data.to_dict('records')
        custom_success = custom_index.build_index_from_data(data_list)
        custom_setup_time = time.time() - custom_setup_start
        
        if not custom_success:
            print(f"❌ Falló construcción índice personalizado para {size} registros")
            return
        
        # Ejecutar benchmarks para cada consulta
        benchmark_data = {
            'dataset_size': size,
            'setup_times': {
                'postgresql_setup': pg_setup_time,
                'custom_index_setup': custom_setup_time
            },
            'query_results': []
        }
        
        print(f"📊 Ejecutando {len(self.test_queries)} consultas de prueba...")
        
        for query in self.test_queries:
            print(f"  🔍 Consulta: '{query}'")
            
            # Benchmark PostgreSQL
            pg_time, pg_results = self.benchmark_postgresql(table_name, query)
            
            # Benchmark índice personalizado  
            custom_time, custom_results = self.benchmark_custom_index(custom_index, query)
            
            # Comparar calidad de resultados
            quality_metrics = self.compare_result_quality(pg_results, custom_results)
            
            query_data = {
                'query': query,
                'postgresql': {
                    'time_seconds': pg_time,
                    'results_count': len(pg_results),
                    'top_results': pg_results[:3]  # Primeros 3 para análisis
                },
                'custom_index': {
                    'time_seconds': custom_time,
                    'results_count': len(custom_results),
                    'top_results': custom_results[:3]
                },
                'quality_comparison': quality_metrics
            }
            
            benchmark_data['query_results'].append(query_data)
            
            print(f"    ⏱️ PostgreSQL: {pg_time:.4f}s | Personalizado: {custom_time:.4f}s")
            print(f"    📈 Jaccard: {quality_metrics['jaccard_similarity']:.3f} | Top-5 overlap: {quality_metrics['overlap_top5']}")
        
        self.results['benchmarks'].append(benchmark_data)
        
        # Limpiar tabla PostgreSQL
        try:
            conn = psycopg2.connect(**self.pg_config)
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            conn.commit()
            conn.close()
        except:
            pass
        
        print(f"✅ Benchmark completado para {size} registros")
    
    def generate_performance_summary(self):
        """Genera resumen de rendimiento"""
        print("\n📋 RESUMEN DE RENDIMIENTO")
        print("=" * 80)
        
        # Tabla de tiempos promedio
        print(f"{'Tamaño':<10} {'PostgreSQL (ms)':<18} {'Mi Índice (ms)':<18} {'Speedup':<12} {'Jaccard Avg':<12}")
        print("-" * 80)
        
        for benchmark in self.results['benchmarks']:
            size = benchmark['dataset_size']
            
            # Calcular promedios
            pg_times = [q['postgresql']['time_seconds'] for q in benchmark['query_results']]
            custom_times = [q['custom_index']['time_seconds'] for q in benchmark['query_results']]
            jaccard_scores = [q['quality_comparison']['jaccard_similarity'] for q in benchmark['query_results']]
            
            avg_pg = np.mean(pg_times) * 1000  # Convertir a ms
            avg_custom = np.mean(custom_times) * 1000
            speedup = avg_pg / avg_custom if avg_custom > 0 else float('inf')
            avg_jaccard = np.mean(jaccard_scores)
            
            print(f"{size:<10} {avg_pg:<18.2f} {avg_custom:<18.2f} {speedup:<12.2f} {avg_jaccard:<12.3f}")
        
        print("\n🏗️ TIEMPOS DE CONSTRUCCIÓN")
        print("-" * 50)
        print(f"{'Tamaño':<10} {'PostgreSQL (s)':<18} {'Mi Índice (s)':<18}")
        print("-" * 50)
        
        for benchmark in self.results['benchmarks']:
            size = benchmark['dataset_size']
            pg_setup = benchmark['setup_times']['postgresql_setup']
            custom_setup = benchmark['setup_times']['custom_index_setup']
            
            print(f"{size:<10} {pg_setup:<18.2f} {custom_setup:<18.2f}")
    
    def save_results(self, filename: str = None):
        """Guarda resultados en archivo JSON"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"benchmark_results_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Resultados guardados en: {filename}")
    
    def research_postgresql_implementation(self):
        """Investiga e imprime información sobre la implementación de PostgreSQL"""
        print("\n🔬 INVESTIGACIÓN: IMPLEMENTACIÓN DE POSTGRESQL")
        print("=" * 80)
        
        try:
            conn = psycopg2.connect(**self.pg_config)
            cur = conn.cursor()
            
            # Información sobre índices
            print("📚 TIPOS DE ÍNDICE PARA BÚSQUEDA DE TEXTO:")
            print("• GIN (Generalized Inverted Index): Optimizado para búsqueda de texto")
            print("• GiST (Generalized Search Tree): Más flexible pero menos eficiente para texto")
            print("• Elegimos GIN para mejor rendimiento en búsquedas de texto\n")
            
            # Información sobre ranking
            print("📊 FUNCIONES DE RANKING:")
            print("• ts_rank(): Ranking básico basado en frecuencia de términos")
            print("• ts_rank_cd(): Ranking con consideración de distancia entre términos")
            print("• Usamos ts_rank_cd() para mejor calidad de resultados\n")
            
            # Verificar configuración
            cur.execute("SELECT cfgname, cfgowner FROM pg_ts_config WHERE cfgname LIKE '%spanish%';")
            configs = cur.fetchall()
            
            print("⚙️ CONFIGURACIONES DE TEXTO INSTALADAS:")
            for cfg in configs:
                print(f"• {cfg[0]} (owner: {cfg[1]})")
            
            # Información sobre procesamiento
            print("\n🔧 PROCESAMIENTO DE CONSULTAS:")
            print("• tsvector: Representa documento como vector de términos normalizados")
            print("• tsquery: Representa consulta como expresión de búsqueda")
            print("• plainto_tsquery(): Convierte texto plano a consulta estructurada")
            print("• Stemming automático y eliminación de stopwords\n")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error investigando PostgreSQL: {e}")
    
    def run_full_benchmark(self):
        """Ejecuta benchmark completo"""
        print("🎯 INICIANDO BENCHMARK COMPLETO DE BÚSQUEDA DE TEXTO")
        print("=" * 80)
        
        # Configurar PostgreSQL
        if not self.setup_postgresql():
            print("❌ Falló configuración inicial de PostgreSQL")
            return
        
        # Cargar dataset
        print("📁 Cargando dataset de Spotify...")
        try:
            data = pd.read_csv('datos/spotify_songs.csv')
            print(f"✅ Dataset cargado: {len(data)} registros")
        except Exception as e:
            print(f"❌ Error cargando dataset: {e}")
            return
        
        # Investigar implementación PostgreSQL
        self.research_postgresql_implementation()
        
        # Ejecutar benchmarks para cada tamaño
        for size in self.dataset_sizes:
            if size <= len(data):
                self.run_benchmark_for_size(data, size)
            else:
                print(f"⚠️ Saltando tamaño {size} (dataset solo tiene {len(data)} registros)")
        
        # Generar resumen
        self.generate_performance_summary()
        
        # Guardar resultados
        self.save_results()
        
        print("\n🎉 ¡Benchmark completado exitosamente!")

def main():
    """Función principal"""
    benchmark = TextSearchBenchmark()
    benchmark.run_full_benchmark()

if __name__ == "__main__":
    main()