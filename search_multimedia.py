#!/usr/bin/env python3
"""
Script para búsqueda de imágenes similares usando SIFT
"""

import os
import sys
from multimedia.multimedia_engine import MultimediaEngine
import time

def search_similar_images(index_path: str, query_image: str, k: int = 10, method: str = 'inverted'):
    """
    Busca imágenes similares usando el índice multimedia
    
    Args:
        index_path: ruta al sistema multimedia guardado
        query_image: ruta de la imagen de consulta
        k: número de resultados
        method: 'sequential' o 'inverted'
    """
    print("=" * 80)
    print("🔍 BÚSQUEDA DE IMÁGENES SIMILARES")
    print("=" * 80)
    
    # Verificar que existe el índice
    if not os.path.exists(index_path):
        print(f"❌ No se encontró el índice en: {index_path}")
        return
    
    # Verificar imagen de consulta
    if not os.path.exists(query_image):
        print(f"❌ No se encontró la imagen: {query_image}")
        return
    
    print(f"\n📂 Cargando índice desde: {index_path}")
    
    # Cargar el motor multimedia
    engine = MultimediaEngine(media_type='image', feature_method='sift')
    
    try:
        engine.load_complete_system(index_path)
        print("✅ Sistema multimedia cargado correctamente")
    except Exception as e:
        print(f"❌ Error cargando el sistema: {e}")
        return
    
    # Realizar búsqueda
    print(f"\n🔍 Buscando imágenes similares a: {query_image}")
    print(f"📊 Método: {method}")
    print(f"📊 Resultados solicitados: {k}")
    
    try:
        start_time = time.time()
        results = engine.search_similar(query_image, k=k, method=method)
        search_time = time.time() - start_time
        
        print(f"\n✅ Búsqueda completada en {search_time:.3f} segundos")
        print(f"📊 Resultados encontrados: {len(results)}")
        
        # Mostrar resultados
        print("\n" + "=" * 80)
        print("🏆 IMÁGENES MÁS SIMILARES:")
        print("=" * 80)
        
        for i, (image_path, similarity) in enumerate(results, 1):
            print(f"\n{i}. Similitud: {similarity:.4f}")
            print(f"   Archivo: {image_path}")
            
            # Mostrar información adicional si está disponible
            if os.path.exists(image_path):
                size_kb = os.path.getsize(image_path) / 1024
                print(f"   Tamaño: {size_kb:.1f} KB")
        
    except Exception as e:
        print(f"❌ Error durante la búsqueda: {e}")
        import traceback
        traceback.print_exc()

def benchmark_search_methods(index_path: str, query_image: str, k: int = 10):
    """Compara el rendimiento de búsqueda secuencial vs índice invertido"""
    print("\n" + "=" * 80)
    print("⚡ BENCHMARK DE MÉTODOS DE BÚSQUEDA")
    print("=" * 80)
    
    engine = MultimediaEngine(media_type='image', feature_method='sift')
    
    try:
        engine.load_complete_system(index_path)
        results = engine.benchmark_search_methods(query_image, k)
        
        print("\n📊 RESULTADOS DEL BENCHMARK:")
        print(f"\n🔵 Búsqueda Secuencial:")
        print(f"   Tiempo: {results['sequential']['time']:.3f} segundos")
        print(f"   Documentos procesados: {results['sequential']['stats']['num_documents']}")
        
        print(f"\n🟢 Búsqueda con Índice Invertido:")
        print(f"   Tiempo: {results['inverted']['time']:.3f} segundos")
        print(f"   Documentos candidatos: Optimizado")
        
        print(f"\n⚡ Speedup: {results['speedup']:.2f}x más rápido con índice invertido")
        
    except Exception as e:
        print(f"❌ Error en benchmark: {e}")

def interactive_search():
    """Modo interactivo de búsqueda"""
    print("🔍 BÚSQUEDA INTERACTIVA DE IMÁGENES SIMILARES")
    print("=" * 50)
    
    # Listar índices disponibles
    embeddings_dir = "embeddings"
    multimedia_dirs = [d for d in os.listdir(embeddings_dir) 
                      if os.path.isdir(os.path.join(embeddings_dir, d)) and d.endswith('_multimedia')]
    
    if not multimedia_dirs:
        print("❌ No se encontraron índices multimedia en embeddings/")
        return
    
    print("\n📂 Índices multimedia disponibles:")
    for i, dir_name in enumerate(multimedia_dirs, 1):
        table_name = dir_name.replace('_multimedia', '')
        print(f"  {i}. {table_name}")
    
    # Seleccionar índice
    choice = input("\nSeleccione el índice (número): ").strip()
    try:
        index_idx = int(choice) - 1
        index_path = os.path.join(embeddings_dir, multimedia_dirs[index_idx])
        table_name = multimedia_dirs[index_idx].replace('_multimedia', '')
    except:
        print("❌ Selección inválida")
        return
    
    print(f"\n✅ Usando índice: {table_name}")
    
    # Loop de búsqueda
    while True:
        print("\n" + "-" * 50)
        query_image = input("\n🖼️  Ruta de la imagen de consulta (o 'exit' para salir): ").strip()
        
        if query_image.lower() == 'exit':
            break
        
        if not os.path.exists(query_image):
            print("❌ La imagen no existe")
            continue
        
        k = input("📊 Número de resultados (Enter = 10): ").strip()
        k = int(k) if k else 10
        
        method = input("📊 Método [1=Invertido, 2=Secuencial, 3=Benchmark] (Enter = 1): ").strip()
        
        if method == '3':
            benchmark_search_methods(index_path, query_image, k)
        else:
            method_name = 'sequential' if method == '2' else 'inverted'
            search_similar_images(index_path, query_image, k, method_name)

def main():
    """Función principal"""
    if len(sys.argv) > 1:
        # Modo línea de comandos
        if len(sys.argv) < 3:
            print("Uso: python search_multimedia.py <index_path> <query_image> [k] [method]")
            print("  index_path: ruta al índice multimedia (ej: embeddings/fashion_multimedia)")
            print("  query_image: ruta de la imagen de consulta")
            print("  k: número de resultados (default: 10)")
            print("  method: 'inverted' o 'sequential' (default: inverted)")
            return
        
        index_path = sys.argv[1]
        query_image = sys.argv[2]
        k = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        method = sys.argv[4] if len(sys.argv) > 4 else 'inverted'
        
        search_similar_images(index_path, query_image, k, method)
    else:
        # Modo interactivo
        interactive_search()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Búsqueda interrumpida")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()