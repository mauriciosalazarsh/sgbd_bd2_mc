#!/usr/bin/env python3
"""
Benchmark rápido para probar TU API multimedia con datasets pequeños
"""

import requests
import time
import os
import pandas as pd
import json
from datetime import datetime

class QuickBenchmark:
    
    def __init__(self, api_url="http://localhost:8000"):
        self.api_url = api_url
        self.results = []
        
    def benchmark_dataset(self, csv_path, dataset_type, media_type, feature_method):
        """Benchmark de un dataset específico"""
        
        print(f"\n🎯 BENCHMARK: {dataset_type.upper()}")
        print("-" * 50)
        
        if not os.path.exists(csv_path):
            print(f"❌ Dataset no encontrado: {csv_path}")
            return
        
        # Leer dataset
        df = pd.read_csv(csv_path)
        size = len(df)
        table_name = f"quick_{dataset_type}_{size}"
        
        print(f"📊 Dataset: {size} muestras")
        print(f"🔧 Método: {feature_method}")
        
        # Resultado del benchmark
        benchmark_result = {
            'dataset': dataset_type,
            'size': size,
            'feature_method': feature_method,
            'csv_path': csv_path,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # 1. Crear tabla
            print("1. 🔨 Creando tabla multimedia...")
            create_data = {
                "table_name": table_name,
                "csv_file_path": csv_path,
                "media_type": media_type,
                "feature_method": feature_method,
                "n_clusters": max(10, size // 10)
            }
            
            start_time = time.time()
            response = requests.post(
                f"{self.api_url}/multimedia/create-table",
                json=create_data,
                timeout=300
            )
            
            if response.status_code != 200:
                print(f"   ❌ Error {response.status_code}: {response.text[:200]}")
                return
            
            result = response.json()
            construction_time = time.time() - start_time
            
            print(f"   ✅ Tabla creada en {construction_time:.2f}s")
            print(f"   📈 Features: {result['data'].get('multimedia_info', {}).get('features_extracted', 'N/A')}")
            
            benchmark_result['construction_time'] = construction_time
            benchmark_result['features_extracted'] = result['data'].get('multimedia_info', {}).get('features_extracted', 0)
            
            # 2. Obtener archivos para consultas
            print("2. 🔍 Preparando consultas...")
            query_files = []
            
            for i, row in df.head(5).iterrows():  # Solo 5 consultas
                if dataset_type == 'fashion':
                    file_path = row.get('image_path', '')
                else:  # audio
                    file_path = row.get('audio_path', '')
                
                if file_path and os.path.exists(file_path):
                    query_files.append(file_path)
            
            if not query_files:
                print("   ❌ No se encontraron archivos válidos para consulta")
                return
            
            print(f"   📁 {len(query_files)} archivos de consulta preparados")
            
            # 3. Benchmark Sequential vs Inverted
            for method in ["sequential", "inverted"]:
                print(f"3. ⚡ Benchmark método {method.upper()}...")
                
                times = []
                successful_queries = 0
                
                for query_file in query_files:
                    try:
                        search_data = {
                            "table_name": table_name,
                            "query_file_path": query_file,
                            "k": 8,
                            "method": method,
                            "fields": ["*"]
                        }
                        
                        start_time = time.time()
                        response = requests.post(
                            f"{self.api_url}/multimedia/search",
                            json=search_data,
                            timeout=60
                        )
                        search_time = time.time() - start_time
                        
                        if response.status_code == 200:
                            result = response.json()
                            count = result['data'].get('count', 0)
                            times.append(search_time)
                            successful_queries += 1
                        else:
                            print(f"     ⚠️ Error en consulta: {response.status_code}")
                            
                    except Exception as e:
                        print(f"     ⚠️ Error: {e}")
                
                if times:
                    avg_time = sum(times) / len(times)
                    min_time = min(times)
                    max_time = max(times)
                    
                    print(f"   ✅ {method}: {avg_time:.4f}s promedio ({successful_queries}/{len(query_files)} consultas)")
                    print(f"      📊 Min: {min_time:.4f}s, Max: {max_time:.4f}s")
                    
                    benchmark_result[f'{method}_avg_time'] = avg_time
                    benchmark_result[f'{method}_min_time'] = min_time
                    benchmark_result[f'{method}_max_time'] = max_time
                    benchmark_result[f'{method}_successful_queries'] = successful_queries
                else:
                    print(f"   ❌ {method}: No se completaron consultas exitosas")
                    benchmark_result[f'{method}_avg_time'] = None
            
            # 4. Usar endpoint de benchmark si está disponible
            print("4. 🚀 Probando endpoint de benchmark...")
            try:
                benchmark_data = {
                    "table_name": table_name,
                    "query_file_path": query_files[0],
                    "k": 8
                }
                
                response = requests.post(
                    f"{self.api_url}/multimedia/benchmark",
                    json=benchmark_data,
                    timeout=120
                )
                
                if response.status_code == 200:
                    api_benchmark = response.json()['data'].get('benchmark_results', {})
                    benchmark_result['api_benchmark'] = api_benchmark
                    print(f"   ✅ Benchmark API completado")
                    
                    for method, stats in api_benchmark.items():
                        if isinstance(stats, dict) and 'avg_time' in stats:
                            print(f"      {method}: {stats['avg_time']:.4f}s")
                else:
                    print(f"   ⚠️ Benchmark API no disponible")
                    
            except Exception as e:
                print(f"   ⚠️ Benchmark API error: {e}")
            
            # Comparación de métodos
            if benchmark_result.get('sequential_avg_time') and benchmark_result.get('inverted_avg_time'):
                seq_time = benchmark_result['sequential_avg_time']
                inv_time = benchmark_result['inverted_avg_time']
                speedup = seq_time / inv_time if inv_time > 0 else 1
                
                print(f"\n📈 RESUMEN:")
                print(f"   Sequential: {seq_time:.4f}s")
                print(f"   Inverted:   {inv_time:.4f}s")
                print(f"   Speedup:    {speedup:.2f}x {'(inverted más rápido)' if speedup > 1 else '(sequential más rápido)'}")
                
                benchmark_result['speedup'] = speedup
            
            self.results.append(benchmark_result)
            
        except Exception as e:
            print(f"❌ Error en benchmark: {e}")
            benchmark_result['error'] = str(e)
            self.results.append(benchmark_result)
    
    def run_quick_benchmark(self):
        """Ejecuta benchmark rápido"""
        print("🚀 BENCHMARK RÁPIDO DE TU API MULTIMEDIA")
        print("=" * 60)
        
        # Health check
        try:
            response = requests.get(f"{self.api_url}/health", timeout=10)
            health = response.json()
            print(f"✅ API Status: {health['data']['status']}")
        except Exception as e:
            print(f"❌ Error conectando a API: {e}")
            return
        
        # Datasets pequeños para benchmark
        datasets = [
            ("datos/fashion_demo_100.csv", "fashion", "image", "sift"),
            ("datos/fma_demo_100.csv", "audio", "audio", "mfcc")
        ]
        
        for csv_path, dataset_type, media_type, feature_method in datasets:
            self.benchmark_dataset(csv_path, dataset_type, media_type, feature_method)
        
        # Guardar resultados
        self.save_results()
        print(f"\n🎉 Benchmark completado! Resultados guardados.")
    
    def save_results(self):
        """Guarda resultados en JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"quick_benchmark_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'api_url': self.api_url,
                'results': self.results
            }, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Resultados guardados en: {filename}")

if __name__ == "__main__":
    benchmark = QuickBenchmark()
    benchmark.run_quick_benchmark()