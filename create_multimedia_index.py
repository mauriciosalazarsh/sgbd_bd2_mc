#!/usr/bin/env python3
"""
Script para crear índice multimedia SIFT con gestión eficiente de memoria
"""

import os
import sys
import gc
import pickle
import numpy as np
from typing import List, Tuple
import pandas as pd

def create_sift_multimedia_index(csv_path: str, image_column: str, table_name: str, 
                                sample_size: int = None, batch_size: int = 100):
    """
    Crea un índice multimedia completo con SIFT
    
    Args:
        csv_path: ruta del CSV
        image_column: columna con rutas de imágenes
        table_name: nombre de la tabla
        sample_size: número de imágenes a procesar (None = todas)
        batch_size: tamaño del lote para procesamiento
    """
    print("=" * 80)
    print("🎨 CREACIÓN DE ÍNDICE MULTIMEDIA CON SIFT")
    print("=" * 80)
    
    # Paso 1: Leer dataset y preparar rutas
    print("\n📊 Leyendo dataset...")
    df = pd.read_csv(csv_path, encoding='latin1')
    total_rows = len(df)
    print(f"Total de registros: {total_rows}")
    
    # Filtrar imágenes válidas
    valid_images = []
    for idx, row in df.iterrows():
        if pd.notna(row[image_column]) and os.path.exists(row[image_column]):
            valid_images.append((idx, row[image_column]))
        
        if sample_size and len(valid_images) >= sample_size:
            break
    
    actual_size = len(valid_images)
    print(f"Imágenes válidas encontradas: {actual_size}")
    
    if actual_size == 0:
        print("❌ No se encontraron imágenes válidas")
        return False
    
    # Paso 2: Configurar el motor multimedia
    print("\n⚙️  Configurando motor multimedia...")
    from multimedia.multimedia_engine import MultimediaEngine
    
    multimedia_engine = MultimediaEngine(
        media_type='image',
        feature_method='sift',
        n_clusters=256  # Puedes reducir esto para menos memoria
    )
    
    # Paso 3: Extraer características en lotes
    print(f"\n🖼️  Extrayendo características SIFT de {actual_size} imágenes...")
    print(f"Procesando en lotes de {batch_size} imágenes")
    
    all_features = []
    features_dir = "embeddings/temp_features"
    os.makedirs(features_dir, exist_ok=True)
    
    for i in range(0, actual_size, batch_size):
        batch_end = min(i + batch_size, actual_size)
        batch_images = [path for _, path in valid_images[i:batch_end]]
        
        print(f"\n📦 Lote {i//batch_size + 1}: imágenes {i+1}-{batch_end}")
        
        try:
            # Extraer características del lote
            batch_features = multimedia_engine.feature_extractor.extract_features_batch(batch_images)
            
            # Guardar lote en archivo temporal
            batch_file = f"{features_dir}/batch_{i//batch_size}.pkl"
            with open(batch_file, 'wb') as f:
                pickle.dump(batch_features, f)
            
            print(f"✅ Lote procesado: {len(batch_features)} características extraídas")
            
            # Liberar memoria
            del batch_features
            gc.collect()
            
        except Exception as e:
            print(f"❌ Error en lote: {e}")
            continue
    
    # Paso 4: Cargar todas las características y construir codebook
    print("\n📚 Construyendo codebook con todas las características...")
    
    all_features = []
    for file in os.listdir(features_dir):
        if file.endswith('.pkl'):
            with open(f"{features_dir}/{file}", 'rb') as f:
                batch_features = pickle.load(f)
                all_features.extend(batch_features)
    
    print(f"Total de características cargadas: {len(all_features)}")
    
    if not all_features:
        print("❌ No se extrajeron características")
        return False
    
    # Guardar todas las características
    features_path = f"embeddings/{table_name}_features.pkl"
    with open(features_path, 'wb') as f:
        pickle.dump(all_features, f)
    
    multimedia_engine.features_data = all_features
    
    # Paso 5: Construir codebook
    print("\n🔨 Construyendo codebook (esto puede tomar tiempo)...")
    try:
        codebook = multimedia_engine.build_codebook(
            save_codebook=True,
            codebook_path=f"embeddings/{table_name}_codebook.pkl"
        )
        print("✅ Codebook construido")
    except Exception as e:
        print(f"❌ Error construyendo codebook: {e}")
        return False
    
    # Paso 6: Crear histogramas
    print("\n📊 Creando histogramas Bag-of-Words...")
    try:
        histograms = multimedia_engine.create_histograms(
            save_histograms=True,
            histograms_path=f"embeddings/{table_name}_histograms.pkl"
        )
        print(f"✅ Histogramas creados: {len(histograms)}")
    except Exception as e:
        print(f"❌ Error creando histogramas: {e}")
        return False
    
    # Paso 7: Construir índices de búsqueda
    print("\n🔍 Construyendo índices de búsqueda KNN...")
    try:
        multimedia_engine.build_search_indices()
        print("✅ Índices de búsqueda construidos")
    except Exception as e:
        print(f"❌ Error construyendo índices: {e}")
        return False
    
    # Paso 8: Guardar sistema completo
    print("\n💾 Guardando sistema multimedia completo...")
    try:
        multimedia_engine.save_complete_system(f"embeddings/{table_name}_multimedia")
        print(f"✅ Sistema guardado en: embeddings/{table_name}_multimedia/")
    except Exception as e:
        print(f"❌ Error guardando sistema: {e}")
        return False
    
    # Limpiar archivos temporales
    print("\n🧹 Limpiando archivos temporales...")
    import shutil
    if os.path.exists(features_dir):
        shutil.rmtree(features_dir)
    
    print("\n" + "=" * 80)
    print("🎉 ÍNDICE MULTIMEDIA CREADO EXITOSAMENTE!")
    print("=" * 80)
    print(f"\n📂 Archivos creados en embeddings/:")
    print(f"  - {table_name}_features.pkl")
    print(f"  - {table_name}_codebook.pkl") 
    print(f"  - {table_name}_histograms.pkl")
    print(f"  - {table_name}_multimedia/ (sistema completo)")
    
    print("\n🔍 Ahora puedes hacer búsquedas similares con:")
    print(f"  engine = MultimediaEngine('image', 'sift')")
    print(f"  engine.load_complete_system('embeddings/{table_name}_multimedia')")
    print(f"  results = engine.search_similar('query_image.jpg', k=10)")
    
    return True

def main():
    """Función principal interactiva"""
    print("🎨 CREADOR DE ÍNDICE MULTIMEDIA SIFT")
    print("=" * 50)
    
    # Solicitar parámetros
    csv_path = input("\n📁 Ruta del archivo CSV: ").strip()
    if not os.path.exists(csv_path):
        print("❌ El archivo no existe")
        return
    
    # Mostrar columnas disponibles
    df_preview = pd.read_csv(csv_path, nrows=5, encoding='latin1')
    print("\n📊 Columnas disponibles:")
    for i, col in enumerate(df_preview.columns):
        print(f"  {i}: {col}")
    
    image_column = input("\n🖼️  Nombre de la columna con rutas de imágenes: ").strip()
    if image_column not in df_preview.columns:
        print("❌ Columna no encontrada")
        return
    
    table_name = input("\n📋 Nombre para la tabla: ").strip()
    if not table_name.isidentifier():
        print("❌ Nombre inválido")
        return
    
    # Preguntar tamaño de muestra
    total_rows = len(pd.read_csv(csv_path, encoding='latin1'))
    print(f"\n📊 El dataset tiene {total_rows} registros")
    print("⚠️  ADVERTENCIA: Procesar todas las imágenes puede usar mucha memoria")
    
    sample_input = input(f"\n¿Cuántas imágenes procesar? (Enter = todas, o número): ").strip()
    if sample_input:
        try:
            sample_size = int(sample_input)
            print(f"✅ Procesando {sample_size} imágenes")
        except:
            sample_size = None
            print("✅ Procesando todas las imágenes")
    else:
        sample_size = None
        print("✅ Procesando todas las imágenes")
    
    # Tamaño de lote
    batch_input = input("\n📦 Tamaño de lote (Enter = 100): ").strip()
    batch_size = int(batch_input) if batch_input else 100
    
    # Confirmar
    print("\n" + "=" * 50)
    print("📋 RESUMEN:")
    print(f"  CSV: {csv_path}")
    print(f"  Columna: {image_column}")
    print(f"  Tabla: {table_name}")
    print(f"  Imágenes a procesar: {sample_size if sample_size else 'Todas'}")
    print(f"  Tamaño de lote: {batch_size}")
    print("=" * 50)
    
    confirm = input("\n¿Continuar? (s/n): ").strip().lower()
    if confirm != 's':
        print("❌ Cancelado")
        return
    
    # Crear índice
    success = create_sift_multimedia_index(
        csv_path=csv_path,
        image_column=image_column,
        table_name=table_name,
        sample_size=sample_size,
        batch_size=batch_size
    )
    
    if success:
        print("\n✅ Proceso completado exitosamente!")
    else:
        print("\n❌ El proceso falló")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()