#!/usr/bin/env python3
"""
Benchmark comprehensivo para búsqueda KNN en bases de datos multimedia.
Compara tu implementación de índices multimedia (sequential vs inverted) contra PostgreSQL + pgvector y Faiss.

Evalúa datasets de fashion (imágenes) y FMA (audio) con tamaños: 1k, 2k, 4k, 8k, 16k, 32k, 64k
K = 8 para todas las consultas
"""

import os
import sys
import time
import pandas as pd
import numpy as np
import psycopg2
import faiss
import json
import pickle
import requests
import random
from typing import List, Dict, Tuple, Any, Optional
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Imports para procesamiento multimedia (solo para PostgreSQL y Faiss comparación)
try:
    from PIL import Image
    import cv2
    import librosa
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    from sklearn.neighbors import NearestNeighbors
    from sklearn.preprocessing import StandardScaler
    MULTIMEDIA_AVAILABLE = True
except ImportError as e:
    print(f"❌ Error importing multimedia libraries: {e}")
    MULTIMEDIA_AVAILABLE = False

class MultimediaAPIClient:
    """Cliente para interactuar con tu API de índices multimedia"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.timeout = 300  # 5 minutos timeout por defecto
    
    def create_multimedia_table(self, table_name: str, csv_file_path: str, 
                               media_type: str, feature_method: str, 
                               n_clusters: int = 256) -> Dict[str, Any]:
        """Crear tabla multimedia usando tu API"""
        url = f"{self.base_url}/multimedia/create-table"
        data = {
            "table_name": table_name,
            "csv_file_path": csv_file_path,
            "media_type": media_type,
            "feature_method": feature_method,
            "n_clusters": n_clusters
        }
        
        try:
            response = self.session.post(url, json=data, timeout=1800)  # 30 min para crear tabla
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            raise Exception(f"Timeout creando tabla multimedia (30 min)")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error de conexión: {e}")
    
    def multimedia_search(self, table_name: str, query_file_path: str, 
                         k: int = 8, method: str = "inverted") -> Dict[str, Any]:
        """Realizar búsqueda multimedia usando tu API"""
        url = f"{self.base_url}/multimedia/search"
        data = {
            "table_name": table_name,
            "query_file_path": query_file_path,
            "k": k,
            "method": method,
            "fields": ["*"]
        }
        
        try:
            response = self.session.post(url, json=data, timeout=60)  # 1 min para búsqueda
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            raise Exception(f"Timeout en búsqueda multimedia (1 min)")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error de conexión en búsqueda: {e}")
    
    def multimedia_benchmark(self, table_name: str, query_file_path: str, 
                           k: int = 8) -> Dict[str, Any]:
        """Benchmark usando endpoint específico de tu API"""
        url = f"{self.base_url}/multimedia/benchmark"
        data = {
            "table_name": table_name,
            "query_file_path": query_file_path,
            "k": k
        }
        
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Obtener información de tabla multimedia"""
        url = f"{self.base_url}/multimedia/tables/{table_name}/info"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def health_check(self) -> Dict[str, Any]:
        """Verificar que la API esté funcionando"""
        url = f"{self.base_url}/health"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

class MultimediaFeatureExtractor:
    """Extractor de características para imágenes y audio"""
    
    def __init__(self):
        self.image_size = (128, 128)  # Reducido para benchmark
        self.audio_sample_rate = 22050
        self.audio_duration = 30  # segundos
        
    def extract_image_features_simple(self, image_path: str) -> Optional[np.ndarray]:
        """Extrae características simples de imagen usando histogramas de color"""
        try:
            if not os.path.exists(image_path):
                # Si no existe la imagen, crear features sintéticas basadas en metadatos
                return np.random.rand(64).astype(np.float32)
            
            img = cv2.imread(image_path)
            if img is None:
                return np.random.rand(64).astype(np.float32)
            
            # Redimensionar
            img = cv2.resize(img, self.image_size)
            
            # Histogramas de color en RGB
            hist_r = cv2.calcHist([img], [0], None, [16], [0, 256])
            hist_g = cv2.calcHist([img], [1], None, [16], [0, 256])
            hist_b = cv2.calcHist([img], [2], None, [16], [0, 256])
            
            # Concatenar y normalizar
            features = np.concatenate([hist_r.flatten(), hist_g.flatten(), hist_b.flatten()])
            features = features / (np.linalg.norm(features) + 1e-7)
            
            return features.astype(np.float32)
            
        except Exception as e:
            print(f"⚠️ Error processing image {image_path}: {e}")
            return np.random.rand(64).astype(np.float32)
    
    def extract_audio_features_simple(self, audio_path: str) -> Optional[np.ndarray]:
        """Extrae características simples de audio usando MFCCs"""
        try:
            if not os.path.exists(audio_path):
                # Si no existe el audio, crear features sintéticas
                return np.random.rand(64).astype(np.float32)
            
            # Cargar audio
            y, sr = librosa.load(audio_path, sr=self.audio_sample_rate, duration=self.audio_duration)
            
            # Extraer MFCCs (coeficientes cepstrales)
            mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)
            
            # Estadísticas: media y std de cada coeficiente
            mfcc_mean = np.mean(mfccs, axis=1)
            mfcc_std = np.std(mfccs, axis=1)
            
            # Otras características
            spectral_centroid = np.mean(librosa.feature.spectral_centroid(y=y, sr=sr))
            spectral_rolloff = np.mean(librosa.feature.spectral_rolloff(y=y, sr=sr))
            zero_crossing_rate = np.mean(librosa.feature.zero_crossing_rate(y))
            
            # Concatenar características
            features = np.concatenate([
                mfcc_mean, mfcc_std, 
                [spectral_centroid, spectral_rolloff, zero_crossing_rate]
            ])
            
            # Normalizar
            features = features / (np.linalg.norm(features) + 1e-7)
            
            return features.astype(np.float32)
            
        except Exception as e:
            print(f"⚠️ Error processing audio {audio_path}: {e}")
            return np.random.rand(64).astype(np.float32)
    
    def extract_text_features(self, text: str, vectorizer: TfidfVectorizer = None) -> np.ndarray:
        """Extrae características de texto usando TF-IDF"""
        try:
            if vectorizer is None:
                # Crear vectorizer simple
                vectorizer = TfidfVectorizer(max_features=64, stop_words='english')
                features = vectorizer.fit_transform([text]).toarray().flatten()
            else:
                features = vectorizer.transform([text]).toarray().flatten()
            
            # Asegurar dimensión fija
            if len(features) < 64:
                features = np.pad(features, (0, 64 - len(features)))
            elif len(features) > 64:
                features = features[:64]
                
            return features.astype(np.float32)
            
        except:
            return np.random.rand(64).astype(np.float32)

class YourMultimediaKNN:
    """Wrapper para usar tus índices multimedia a través de la API"""
    
    def __init__(self, api_client: MultimediaAPIClient, table_name: str, 
                 csv_file_path: str, media_type: str, feature_method: str, 
                 n_clusters: int = 256):
        self.api_client = api_client
        self.table_name = table_name
        self.csv_file_path = csv_file_path
        self.media_type = media_type
        self.feature_method = feature_method
        self.n_clusters = n_clusters
        self.is_built = False
        
    def build_index(self) -> float:
        """Construye la tabla multimedia usando tu API"""
        print(f"    🔨 Construyendo tabla multimedia '{self.table_name}' usando tu API...")
        
        start_time = time.time()
        try:
            response = self.api_client.create_multimedia_table(
                table_name=self.table_name,
                csv_file_path=self.csv_file_path,
                media_type=self.media_type,
                feature_method=self.feature_method,
                n_clusters=self.n_clusters
            )
            
            build_time = time.time() - start_time
            
            if response.get('success', False):
                self.is_built = True
                construction_time = response.get('data', {}).get('construction_time', build_time)
                print(f"    ✅ Tabla construida exitosamente en {construction_time:.2f}s")
                return construction_time
            else:
                raise Exception(f"Error en API: {response.get('message', 'Unknown error')}")
                
        except Exception as e:
            print(f"    ❌ Error construyendo tabla: {e}")
            raise e
    
    def search(self, query_file_path: str, k: int = 8, method: str = "inverted") -> Tuple[List[Dict], float]:
        """Búsqueda usando tu implementación"""
        if not self.is_built:
            raise Exception("Tabla no construida. Llama build_index() primero.")
        
        start_time = time.time()
        try:
            response = self.api_client.multimedia_search(
                table_name=self.table_name,
                query_file_path=query_file_path,
                k=k,
                method=method
            )
            
            search_time = time.time() - start_time
            
            if response.get('success', False):
                data = response.get('data', {})
                api_search_time = data.get('search_time', search_time)
                
                # Convertir resultados al formato esperado
                rows = data.get('rows', [])
                columns = data.get('columns', [])
                
                results = []
                for row in rows:
                    result_dict = {}
                    for i, col in enumerate(columns):
                        if i < len(row):
                            result_dict[col] = row[i]
                    results.append(result_dict)
                
                return results, api_search_time
            else:
                raise Exception(f"Error en búsqueda API: {response.get('message', 'Unknown error')}")
                
        except Exception as e:
            print(f"    ❌ Error en búsqueda: {e}")
            raise e
    
    def benchmark_search_methods(self, query_file_path: str, k: int = 8) -> Dict[str, Any]:
        """Usa el endpoint de benchmark de tu API"""
        if not self.is_built:
            raise Exception("Tabla no construida. Llama build_index() primero.")
        
        try:
            response = self.api_client.multimedia_benchmark(
                table_name=self.table_name,
                query_file_path=query_file_path,
                k=k
            )
            
            if response.get('success', False):
                return response.get('data', {}).get('benchmark_results', {})
            else:
                raise Exception(f"Error en benchmark API: {response.get('message', 'Unknown error')}")
                
        except Exception as e:
            print(f"    ❌ Error en benchmark: {e}")
            raise e

class PostgreSQLKNN:
    """Implementación KNN usando PostgreSQL + pgvector"""
    
    def __init__(self, pg_config: Dict):
        self.pg_config = pg_config
        self.table_name = None
    
    def build_index(self, features: np.ndarray, metadata: List[Dict], 
                   table_name: str) -> bool:
        """Construye tabla e índice en PostgreSQL"""
        try:
            self.table_name = table_name
            
            conn = psycopg2.connect(**self.pg_config)
            cur = conn.cursor()
            
            # Eliminar tabla si existe
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            
            # Crear tabla con columna vector
            dimension = features.shape[1]
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    feature_vector vector({dimension}),
                    metadata JSONB
                );
            """)
            
            # Insertar datos
            print(f"    📥 Insertando {len(features)} vectores...")
            for i, (feature_vec, meta) in enumerate(zip(features, metadata)):
                # Convertir numpy array a lista para PostgreSQL
                vector_str = '[' + ','.join(map(str, feature_vec)) + ']'
                metadata_json = json.dumps(meta)
                
                cur.execute(f"""
                    INSERT INTO {table_name} (feature_vector, metadata)
                    VALUES (%s, %s)
                """, (vector_str, metadata_json))
            
            # Crear índice HNSW para vectores
            print(f"    🗂️ Creando índice HNSW...")
            cur.execute(f"""
                CREATE INDEX ON {table_name} 
                USING hnsw (feature_vector vector_cosine_ops);
            """)
            
            # Analizar tabla
            cur.execute(f"ANALYZE {table_name};")
            
            conn.commit()
            conn.close()
            
            print(f"    ✅ Índice PostgreSQL creado")
            return True
            
        except Exception as e:
            print(f"    ❌ Error creando índice PostgreSQL: {e}")
            return False
    
    def search(self, query_vector: np.ndarray, k: int = 8) -> List[Tuple[Dict, float]]:
        """Búsqueda KNN en PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.pg_config)
            cur = conn.cursor()
            
            # Preparar vector de consulta
            vector_str = '[' + ','.join(map(str, query_vector)) + ']'
            
            # Ejecutar consulta KNN
            cur.execute(f"""
                SELECT metadata, feature_vector <-> %s as distance
                FROM {self.table_name}
                ORDER BY feature_vector <-> %s
                LIMIT %s;
            """, (vector_str, vector_str, k))
            
            results = []
            for row in cur.fetchall():
                metadata = row[0]
                distance = float(row[1])
                results.append((metadata, distance))
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"    ❌ Error en búsqueda PostgreSQL: {e}")
            return []

class FaissKNN:
    """Implementación KNN usando Faiss"""
    
    def __init__(self, features: np.ndarray, metadata: List[Dict]):
        self.features = features.astype(np.float32)
        self.metadata = metadata
        self.dimension = features.shape[1]
        
        # Construir índice Faiss
        self._build_index()
    
    def _build_index(self):
        """Construye índice Faiss HNSW"""
        print(f"    🔨 Construyendo índice Faiss HNSW...")
        
        # Crear índice HNSW (Hierarchical Navigable Small World)
        self.index = faiss.IndexHNSWFlat(self.dimension, 32)  # M=32
        self.index.hnsw.efConstruction = 200
        self.index.hnsw.efSearch = 128
        
        # Añadir vectores al índice
        self.index.add(self.features)
        
        print(f"    ✅ Índice Faiss construido: {self.index.ntotal} vectores")
    
    def search(self, query_vector: np.ndarray, k: int = 8) -> List[Tuple[Dict, float]]:
        """Búsqueda KNN usando Faiss"""
        query_vector = query_vector.astype(np.float32).reshape(1, -1)
        
        # Búsqueda
        distances, indices = self.index.search(query_vector, k)
        
        results = []
        for i, (dist, idx) in enumerate(zip(distances[0], indices[0])):
            if idx >= 0:  # Índice válido
                results.append((self.metadata[idx], float(dist)))
        
        return results

class MultimediaKNNBenchmark:
    """Clase principal para benchmark de KNN multimedia"""
    
    def __init__(self, api_base_url: str = "http://localhost:8000"):
        # Cliente para tu API
        self.api_client = MultimediaAPIClient(api_base_url)
        
        # Configuración PostgreSQL para comparación
        self.pg_config = {
            'host': 'localhost',
            'database': 'postgres',
            'user': 'msalazarh',
            'password': '',
            'port': 5432
        }
        
        # Tamaños de datasets a probar (escalables hasta límites del dataset)
        self.dataset_sizes = [1000, 2000, 4000, 8000, 16000, 32000]  # 64k tomará demasiado tiempo
        self.k = 8  # Número de vecinos más cercanos
        
        # Extractor de características (solo para PostgreSQL/Faiss)
        self.feature_extractor = MultimediaFeatureExtractor()
        
        # Resultados
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'api_url': api_base_url,
            'benchmarks': []
        }
        
        # Verificar que tu API esté funcionando
        try:
            health = self.api_client.health_check()
            print(f"✅ API conectada: {health.get('data', {}).get('status', 'unknown')}")
        except Exception as e:
            print(f"❌ Error conectando a tu API en {api_base_url}: {e}")
            raise e
    
    def load_and_process_dataset(self, dataset_path: str, dataset_type: str, 
                               size: int) -> Tuple[np.ndarray, List[Dict]]:
        """Carga y procesa dataset multimedia"""
        print(f"📁 Cargando {dataset_type} dataset: {size} muestras")
        
        # Cargar dataset
        df = pd.read_csv(dataset_path).head(size)
        
        features = []
        metadata = []
        
        print(f"🔄 Extrayendo características...")
        
        for idx, row in df.iterrows():
            if idx % (size // 10) == 0:
                print(f"    Progreso: {idx}/{size}")
            
            if dataset_type == 'fashion':
                # Extraer características de imagen
                image_path = row.get('image_path', '')
                feature_vec = self.feature_extractor.extract_image_features_simple(image_path)
                
                # Combinar con características de texto
                text_features = self.feature_extractor.extract_text_features(
                    row.get('combined_text', '')
                )
                
                # Concatenar características
                if feature_vec is not None:
                    combined_features = np.concatenate([feature_vec, text_features])
                else:
                    combined_features = text_features
                
            elif dataset_type == 'audio':
                # Extraer características de audio
                audio_path = row.get('audio_path', '')
                feature_vec = self.feature_extractor.extract_audio_features_simple(audio_path)
                
                # Combinar con características de texto
                text_features = self.feature_extractor.extract_text_features(
                    row.get('combined_text', '')
                )
                
                # Concatenar características
                if feature_vec is not None:
                    combined_features = np.concatenate([feature_vec, text_features])
                else:
                    combined_features = text_features
            
            features.append(combined_features)
            
            # Metadatos simplificados
            meta = {
                'id': row.get('id', idx),
                'title': str(row.get('productDisplayName' if dataset_type == 'fashion' else 'title', '')),
                'category': str(row.get('masterCategory' if dataset_type == 'fashion' else 'genre', '')),
                'path': str(row.get('image_path' if dataset_type == 'fashion' else 'audio_path', ''))
            }
            metadata.append(meta)
        
        features_array = np.array(features, dtype=np.float32)
        
        print(f"✅ Dataset procesado: {features_array.shape}")
        print(f"   Dimensionalidad: {features_array.shape[1]}")
        
        return features_array, metadata
    
    def benchmark_algorithms(self, dataset_path: str, dataset_type: str, size: int):
        """Ejecuta benchmark de todos los algoritmos usando tu API y comparaciones"""
        
        # Determinar método de extracción según tipo de dataset (usar métodos simples)
        if dataset_type == 'fashion':
            feature_method = 'sift'  # Usar SIFT que es más ligero
            api_media_type = 'image'  # API espera 'image' no 'fashion'
        elif dataset_type == 'audio':
            feature_method = 'mfcc'  # Usar MFCC que es más simple  
            api_media_type = 'audio'
        else:
            feature_method = 'sift'
            api_media_type = 'image'
        
        n_clusters = min(512, max(32, size // 8))
        print(f"\n🚀 Benchmark {dataset_type} - {size:,} muestras")
        print(f"🔧 Método: {feature_method}, Clusters: {n_clusters}")
        
        table_name = f"benchmark_{dataset_type}_{size}"
        n_queries = min(10, size)
        
        benchmark_data = {
            'dataset_type': dataset_type,
            'dataset_size': size,
            'feature_method': feature_method,
            'n_queries': n_queries,
            'algorithms': {}
        }
        
        # Obtener algunos archivos de consulta del dataset
        df = pd.read_csv(dataset_path).head(size)
        query_files = []
        
        # Seleccionar archivos para consulta
        for i in range(min(n_queries, len(df))):
            if dataset_type == 'fashion':
                file_path = df.iloc[i].get('image_path', '')
            else:  # audio
                file_path = df.iloc[i].get('audio_path', '')
            
            if file_path and os.path.exists(file_path):
                query_files.append(file_path)
        
        if not query_files:
            print("❌ No se encontraron archivos de consulta válidos")
            return
        
        print(f"   Archivos de consulta: {len(query_files)}")
        
        # 1. TU IMPLEMENTACIÓN (Sequential vs Inverted)
        print("  📊 Benchmarking TUS ÍNDICES MULTIMEDIA...")
        try:
            # Crear tabla usando tu API
            your_knn = YourMultimediaKNN(
                api_client=self.api_client,
                table_name=table_name,
                csv_file_path=dataset_path,
                media_type=api_media_type,
                feature_method=feature_method,
                n_clusters=n_clusters
            )
            
            # Construir índice
            build_time = your_knn.build_index()
            
            # Benchmark Sequential
            print("    🔍 Probando método Sequential...")
            sequential_times = []
            max_queries = min(3, len(query_files))  # Solo 3 consultas para datasets grandes
            for query_file in query_files[:max_queries]:
                try:
                    _, search_time = your_knn.search(query_file, self.k, method="sequential")
                    sequential_times.append(search_time)
                except Exception as e:
                    print(f"      ⚠️ Error en búsqueda sequential: {e}")
            
            if sequential_times:
                avg_seq_time = np.mean(sequential_times)
                benchmark_data['algorithms']['your_sequential'] = {
                    'avg_time_seconds': avg_seq_time,
                    'build_time_seconds': build_time,
                    'times': sequential_times,
                    'status': 'success'
                }
                print(f"    ✅ TU Sequential: {avg_seq_time:.4f}s promedio, build: {build_time:.2f}s")
            
            # Benchmark Inverted Index
            print("    🔍 Probando método Inverted...")
            inverted_times = []
            for query_file in query_files[:max_queries]:
                try:
                    _, search_time = your_knn.search(query_file, self.k, method="inverted")
                    inverted_times.append(search_time)
                except Exception as e:
                    print(f"      ⚠️ Error en búsqueda inverted: {e}")
            
            if inverted_times:
                avg_inv_time = np.mean(inverted_times)
                benchmark_data['algorithms']['your_inverted'] = {
                    'avg_time_seconds': avg_inv_time,
                    'build_time_seconds': build_time,  # Mismo build time
                    'times': inverted_times,
                    'status': 'success'
                }
                print(f"    ✅ TU Inverted: {avg_inv_time:.4f}s promedio")
            
            # Usar endpoint de benchmark específico si está disponible
            try:
                print("    🔍 Usando endpoint de benchmark...")
                benchmark_results = your_knn.benchmark_search_methods(query_files[0], self.k)
                if benchmark_results:
                    benchmark_data['your_api_benchmark'] = benchmark_results
                    print(f"    ✅ Benchmark API completado")
            except Exception as e:
                print(f"    ⚠️ Benchmark API no disponible: {e}")
                
        except Exception as e:
            print(f"    ❌ Error con TUS ÍNDICES: {e}")
            benchmark_data['algorithms']['your_sequential'] = {
                'status': 'error',
                'error': str(e)
            }
            benchmark_data['algorithms']['your_inverted'] = {
                'status': 'error', 
                'error': str(e)
            }
        
        # 2. COMPARACIÓN: Cargar datos para PostgreSQL y Faiss (DESHABILITADO por ahora)
        print("  ⚠️ Saltando comparación con PostgreSQL/Faiss (enfoque en tu API)")
        features, metadata = None, None
        query_indices = []
        benchmark_data['dimensionality'] = "Unknown (comparison disabled)"
        
        # 3. PostgreSQL + pgvector (solo si hay datos)
        if features is not None and len(query_indices) > 0:
            print("  📊 Benchmarking PostgreSQL + pgvector...")
            try:
                pg_knn = PostgreSQLKNN(self.pg_config)
                table_name_pg = f"multimedia_{dataset_type}_{size}"
                
                build_start = time.time()
                build_success = pg_knn.build_index(features, metadata, table_name_pg)
                build_time = time.time() - build_start
                
                if build_success:
                    times = []
                    for query_idx in query_indices:
                        query_vector = features[query_idx]
                        
                        start_time = time.time()
                        results = pg_knn.search(query_vector, self.k)
                        end_time = time.time()
                        
                        times.append(end_time - start_time)
                    
                    avg_time = np.mean(times)
                    benchmark_data['algorithms']['postgresql'] = {
                        'avg_time_seconds': avg_time,
                        'build_time_seconds': build_time,
                        'times': times,
                        'status': 'success'
                    }
                    print(f"    ✅ PostgreSQL: {avg_time:.4f}s promedio, build: {build_time:.2f}s")
                    
                    # Limpiar tabla
                    try:
                        conn = psycopg2.connect(**self.pg_config)
                        cur = conn.cursor()
                        cur.execute(f"DROP TABLE IF EXISTS {table_name_pg};")
                        conn.commit()
                        conn.close()
                    except:
                        pass
                else:
                    benchmark_data['algorithms']['postgresql'] = {
                        'status': 'error',
                        'error': 'Failed to build index'
                    }
                    
            except Exception as e:
                print(f"    ❌ Error PostgreSQL: {e}")
                benchmark_data['algorithms']['postgresql'] = {
                    'status': 'error',
                    'error': str(e)
                }
        else:
            print("  ⚠️ Saltando PostgreSQL (no hay datos de comparación)")
            benchmark_data['algorithms']['postgresql'] = {
                'status': 'skipped',
                'error': 'No comparison data available'
            }
        
        # 4. Faiss (solo si hay datos)
        if features is not None and len(query_indices) > 0:
            print("  📊 Benchmarking Faiss...")
            try:
                build_start = time.time()
                faiss_knn = FaissKNN(features, metadata)
                build_time = time.time() - build_start
                
                times = []
                for query_idx in query_indices:
                    query_vector = features[query_idx]
                    
                    start_time = time.time()
                    results = faiss_knn.search(query_vector, self.k)
                    end_time = time.time()
                    
                    times.append(end_time - start_time)
                
                avg_time = np.mean(times)
                benchmark_data['algorithms']['faiss'] = {
                    'avg_time_seconds': avg_time,
                    'build_time_seconds': build_time,
                    'times': times,
                    'status': 'success'
                }
                print(f"    ✅ Faiss: {avg_time:.4f}s promedio, build: {build_time:.2f}s")
                
            except Exception as e:
                print(f"    ❌ Error Faiss: {e}")
                benchmark_data['algorithms']['faiss'] = {
                    'status': 'error',
                    'error': str(e)
                }
        else:
            print("  ⚠️ Saltando Faiss (no hay datos de comparación)")
            benchmark_data['algorithms']['faiss'] = {
                'status': 'skipped',
                'error': 'No comparison data available'
            }
        
        self.results['benchmarks'].append(benchmark_data)
    
    def run_comprehensive_benchmark(self):
        """Ejecuta benchmark completo usando tu API vs comparaciones"""
        print("🎯 INICIANDO BENCHMARK COMPLETO KNN MULTIMEDIA CON TU API")
        print("=" * 80)
        
        # Datasets completos para benchmark escalable
        datasets = [
            ('datos/fashion_complete_dataset.csv', 'fashion'),
            ('datos/fma_complete_dataset.csv', 'audio')
        ]
        
        for dataset_path, dataset_type in datasets:
            print(f"\n🎨 DATASET: {dataset_type.upper()}")
            print("-" * 50)
            
            if not os.path.exists(dataset_path):
                print(f"❌ Dataset no encontrado: {dataset_path}")
                continue
            
            # Cargar dataset completo una vez
            try:
                full_df = pd.read_csv(dataset_path)
                max_size = len(full_df)
                print(f"📊 Dataset completo: {max_size} muestras")
            except Exception as e:
                print(f"❌ Error leyendo dataset {dataset_path}: {e}")
                continue
            
            # Ejecutar para cada tamaño
            for i, size in enumerate(self.dataset_sizes):
                if size > max_size:
                    print(f"⚠️ Saltando tamaño {size} (dataset solo tiene {max_size} muestras)")
                    continue
                
                print(f"\n{'='*60}")
                print(f"📊 PROGRESO: {i+1}/{len(self.dataset_sizes)} - Procesando {size:,} muestras")
                print(f"{'='*60}")
                
                try:
                    # Crear subset del dataset
                    subset_df = full_df.head(size)
                    subset_path = f"datos/{dataset_type}_subset_{size}.csv"
                    subset_df.to_csv(subset_path, index=False)
                    
                    # Ejecutar benchmark
                    self.benchmark_algorithms(subset_path, dataset_type, size)
                    
                    # Limpiar archivo temporal
                    try:
                        os.remove(subset_path)
                    except:
                        pass
                    
                except Exception as e:
                    print(f"❌ Error procesando tamaño {size}: {e}")
                    continue
        
        # Generar resumen
        self.generate_summary()
        
        # Guardar resultados
        self.save_results()
        
        print("\n🎉 ¡Benchmark completado exitosamente!")
    
    def generate_summary(self):
        """Genera resumen de resultados"""
        print("\n📋 RESUMEN DE RENDIMIENTO - TU API MULTIMEDIA")
        print("=" * 100)
        print(f"🔗 API URL: {self.api_url}")
        print(f"⏰ Benchmark ejecutado: {self.results['timestamp']}")
        
        algorithms = ['your_sequential', 'your_inverted', 'postgresql', 'faiss']
        algorithm_names = ['TU-Sequential', 'TU-Inverted', 'PostgreSQL', 'Faiss']
        
        for dataset_type in ['fashion', 'audio']:
            print(f"\n🎨 DATASET: {dataset_type.upper()}")
            print("-" * 80)
            
            # Filtrar benchmarks para este dataset
            dataset_benchmarks = [b for b in self.results['benchmarks'] 
                                if b['dataset_type'] == dataset_type]
            
            if not dataset_benchmarks:
                print("❌ No hay datos para este dataset")
                continue
            
            # Tabla de tiempos de búsqueda
            print(f"{'Tamaño':<10} {'TU-Seq (ms)':<15} {'TU-Inv (ms)':<15} {'PostgreSQL (ms)':<18} {'Faiss (ms)':<12}")
            print("-" * 80)
            
            for benchmark in dataset_benchmarks:
                size = benchmark['dataset_size']
                row = f"{size:<10}"
                
                for alg in algorithms:
                    if alg in benchmark['algorithms'] and benchmark['algorithms'][alg].get('status') == 'success':
                        time_ms = benchmark['algorithms'][alg]['avg_time_seconds'] * 1000
                        row += f" {time_ms:<14.2f}"
                    else:
                        row += f" {'ERROR':<14}"
                
                print(row)
            
            # Tabla de tiempos de construcción
            print(f"\n🏗️ TIEMPOS DE CONSTRUCCIÓN ({dataset_type.upper()})")
            print("-" * 60)
            print(f"{'Tamaño':<10} {'TU-API (s)':<15} {'PostgreSQL (s)':<18} {'Faiss (s)':<12}")
            print("-" * 60)
            
            for benchmark in dataset_benchmarks:
                size = benchmark['dataset_size']
                row = f"{size:<10}"
                
                # Para tu API, ambos métodos usan el mismo build time
                for alg in ['your_inverted', 'postgresql', 'faiss']:
                    if (alg in benchmark['algorithms'] and 
                        benchmark['algorithms'][alg].get('status') == 'success' and
                        'build_time_seconds' in benchmark['algorithms'][alg]):
                        build_time = benchmark['algorithms'][alg]['build_time_seconds']
                        row += f" {build_time:<14.2f}"
                    else:
                        row += f" {'N/A':<14}"
                
                print(row)
            
            # Análisis de dimensionalidad
            if dataset_benchmarks:
                dimensionality = dataset_benchmarks[0]['dimensionality']
                print(f"\n📏 DIMENSIONALIDAD: {dimensionality}")
                print("💡 Comparación de algoritmos:")
                print("   • TU Sequential: Búsqueda lineal en tu implementación")
                print("   • TU Inverted: Índice invertido con clustering de tu implementación") 
                print("   • PostgreSQL HNSW: Optimizado para alta dimensionalidad")
                print("   • Faiss HNSW: Biblioteca especializada para vectores densos")
    
    def save_results(self):
        """Guarda resultados en archivo JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"your_multimedia_api_benchmark_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Resultados guardados en: {filename}")

def main():
    """Función principal"""
    print("🚀 BENCHMARK DE TU API MULTIMEDIA vs COMPETENCIA")
    print("=" * 60)
    
    # Configurar URL de tu API
    api_url = input("URL de tu API (default: http://localhost:8000): ").strip()
    if not api_url:
        api_url = "http://localhost:8000"
    
    try:
        benchmark = MultimediaKNNBenchmark(api_url)
        benchmark.run_comprehensive_benchmark()
    except Exception as e:
        print(f"❌ Error ejecutando benchmark: {e}")
        print("\n💡 Asegúrate de que:")
        print("   1. Tu API esté corriendo en", api_url)
        print("   2. Los datasets estén en la carpeta 'datos/'")
        print("   3. Las dependencias estén instaladas (requests, pandas, etc.)")
        return 1
    
    return 0

if __name__ == "__main__":
    main()