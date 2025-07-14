#!/usr/bin/env python3
"""
Test simple para verificar tu API multimedia funcionando
"""

import requests
import time
import os

def test_api(base_url="http://localhost:8000"):
    """Test simple de tu API multimedia"""
    
    print("🧪 TESTING TU API MULTIMEDIA")
    print("=" * 50)
    
    # 1. Health check
    print("1. 🔍 Health check...")
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        health = response.json()
        print(f"   ✅ API Status: {health['data']['status']}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    # 2. Verificar datasets disponibles
    print("\n2. 📁 Verificando datasets...")
    datasets = [
        ("datos/fashion_demo_100.csv", "image", "sift"),
        ("datos/fma_demo_100.csv", "audio", "mfcc")
    ]
    
    for csv_path, media_type, method in datasets:
        if os.path.exists(csv_path):
            print(f"   ✅ {csv_path} encontrado")
        else:
            print(f"   ⚠️ {csv_path} no encontrado, saltando...")
            continue
            
        # 3. Crear tabla multimedia
        table_name = f"test_{media_type}_100"
        print(f"\n3. 🔨 Creando tabla {table_name}...")
        
        try:
            create_data = {
                "table_name": table_name,
                "csv_file_path": csv_path,
                "media_type": media_type,
                "feature_method": method,
                "n_clusters": 20  # Reducido para rapidez
            }
            
            start_time = time.time()
            response = requests.post(
                f"{base_url}/multimedia/create-table", 
                json=create_data, 
                timeout=120
            )
            
            if response.status_code == 200:
                result = response.json()
                construction_time = time.time() - start_time
                print(f"   ✅ Tabla creada en {construction_time:.2f}s")
                print(f"   📊 Features extraídos: {result['data'].get('multimedia_info', {}).get('features_extracted', 'N/A')}")
            else:
                print(f"   ❌ Error {response.status_code}: {response.text}")
                continue
                
        except Exception as e:
            print(f"   ❌ Error creando tabla: {e}")
            continue
        
        # 4. Probar búsquedas
        print(f"\n4. 🔍 Probando búsquedas en {table_name}...")
        
        # Obtener un archivo para consulta (primer archivo del CSV)
        import pandas as pd
        try:
            df = pd.read_csv(csv_path).head(5)
            
            for method_test in ["sequential", "inverted"]:
                print(f"   📊 Método: {method_test}")
                
                for i, row in df.iterrows():
                    if media_type == "image":
                        query_file = row.get('image_path', '')
                    else:
                        query_file = row.get('audio_path', '')
                    
                    if not query_file or not os.path.exists(query_file):
                        continue
                    
                    try:
                        search_data = {
                            "table_name": table_name,
                            "query_file_path": query_file,
                            "k": 5,
                            "method": method_test,
                            "fields": ["*"]
                        }
                        
                        start_time = time.time()
                        response = requests.post(
                            f"{base_url}/multimedia/search",
                            json=search_data,
                            timeout=30
                        )
                        search_time = time.time() - start_time
                        
                        if response.status_code == 200:
                            result = response.json()
                            count = result['data'].get('count', 0)
                            print(f"     ✅ {method_test}: {count} resultados en {search_time:.4f}s")
                            break  # Solo probar con el primer archivo válido
                        else:
                            print(f"     ❌ Error {response.status_code}: {response.text[:100]}")
                            
                    except Exception as e:
                        print(f"     ❌ Error búsqueda: {e}")
                        break
                        
        except Exception as e:
            print(f"   ❌ Error probando búsquedas: {e}")
        
        # 5. Probar benchmark endpoint si está disponible
        print(f"\n5. ⚡ Probando benchmark en {table_name}...")
        try:
            # Usar el primer archivo válido encontrado
            if 'query_file' in locals() and os.path.exists(query_file):
                benchmark_data = {
                    "table_name": table_name,
                    "query_file_path": query_file,
                    "k": 5
                }
                
                response = requests.post(
                    f"{base_url}/multimedia/benchmark",
                    json=benchmark_data,
                    timeout=60
                )
                
                if response.status_code == 200:
                    result = response.json()
                    benchmark_results = result['data'].get('benchmark_results', {})
                    print(f"   ✅ Benchmark completado:")
                    
                    for method, stats in benchmark_results.items():
                        if isinstance(stats, dict) and 'avg_time' in stats:
                            print(f"     {method}: {stats['avg_time']:.4f}s promedio")
                else:
                    print(f"   ⚠️ Benchmark no disponible: {response.status_code}")
            else:
                print(f"   ⚠️ No hay archivo de consulta válido para benchmark")
                
        except Exception as e:
            print(f"   ⚠️ Benchmark no disponible: {e}")
        
        print(f"\n{'='*50}")
    
    print("\n🎉 Test completado!")
    return True

if __name__ == "__main__":
    test_api()