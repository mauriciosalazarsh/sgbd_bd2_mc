#!/usr/bin/env python3
"""
Script CORREGIDO para crear dataset Fashion con rutas válidas para el sistema multimedia
"""

import pandas as pd
import os
from pathlib import Path

def create_unified_fashion_dataset():
    """Crea un CSV unificado para el sistema multimedia - VERSIÓN CORREGIDA"""
    
    # Rutas base
    base_path = "datos/fashion-dataset"
    images_csv = os.path.join(base_path, "images.csv")
    styles_csv = os.path.join(base_path, "styles.csv")
    images_dir = os.path.join(base_path, "images")
    
    print(" Creando dataset unificado para Fashion Dataset...")
    
    # Verificar que existen los archivos
    if not os.path.exists(images_csv):
        print(f" No se encontró: {images_csv}")
        return
    
    if not os.path.exists(styles_csv):
        print(f" No se encontró: {styles_csv}")
        return
    
    if not os.path.exists(images_dir):
        print(f" No se encontró directorio: {images_dir}")
        return
    
    # Cargar CSV con manejo robusto de errores
    print(" Cargando archivos CSV...")
    try:
        images_df = pd.read_csv(images_csv)
        print(f"  - images.csv cargado correctamente")
    except Exception as e:
        print(f" Error cargando images.csv: {e}")
        return
    
    try:
        # Intentar cargar styles.csv con diferentes opciones
        styles_df = pd.read_csv(styles_csv, 
                               quoting=1,  # QUOTE_ALL
                               escapechar='\\',
                               on_bad_lines='skip')  # Saltar líneas problemáticas
        print(f"  - styles.csv cargado correctamente")
    except Exception as e:
        print(f" Error con modo normal, intentando modo alternativo...")
        try:
            # Modo alternativo: leer línea por línea
            styles_df = pd.read_csv(styles_csv, 
                                   on_bad_lines='warn',
                                   quoting=3)  # QUOTE_NONE
            print(f"  - styles.csv cargado en modo alternativo")
        except Exception as e2:
            print(f" Error crítico cargando styles.csv: {e2}")
            return
    
    print(f"  - images.csv: {len(images_df)} filas")
    print(f"  - styles.csv: {len(styles_df)} filas")
    
    # Extraer ID del filename (quitar .jpg)
    images_df['id'] = images_df['filename'].str.replace('.jpg', '').astype(int)
    
    # Unir ambos DataFrames
    print(" Uniendo datos...")
    unified_df = pd.merge(images_df, styles_df, on='id', how='inner')
    print(f"  - Dataset unificado: {len(unified_df)} filas")
    
    # CORREGIDO: Crear ruta completa de imagen (columna correcta para el parser)
    unified_df['image_path'] = unified_df['filename'].apply(
        lambda x: os.path.join(images_dir, x)
    )
    
    # Verificar que las imágenes existen
    print(" Verificando existencia de imágenes...")
    existing_images = []
    missing_count = 0
    
    for idx, row in unified_df.iterrows():
        if os.path.exists(row['image_path']):
            existing_images.append(idx)
        else:
            missing_count += 1
    
    # Filtrar solo imágenes existentes
    final_df = unified_df.loc[existing_images].copy()
    
    print(f"  - Imágenes encontradas: {len(final_df)}")
    print(f"  - Imágenes faltantes: {missing_count}")
    
    # Crear columna de texto combinado para búsqueda textual (SPIMI)
    print(" Creando campos textuales para SPIMI...")
    
    final_df['combined_text'] = (
        final_df['productDisplayName'].fillna('') + ' ' +
        final_df['gender'].fillna('') + ' ' +
        final_df['masterCategory'].fillna('') + ' ' +
        final_df['subCategory'].fillna('') + ' ' +
        final_df['articleType'].fillna('') + ' ' +
        final_df['baseColour'].fillna('') + ' ' +
        final_df['season'].fillna('') + ' ' +
        final_df['usage'].fillna('')
    ).str.strip()
    
    # CORREGIDO: Columnas para el sistema multimedia
    multimedia_columns = [
        'id',
        'image_path',              # CORREGIDO: Esta es la columna que detecta el parser
        'productDisplayName',      # Nombre del producto
        'combined_text',          # Para búsqueda textual SPIMI
        'gender',
        'masterCategory',
        'subCategory', 
        'articleType',
        'baseColour',
        'season',
        'year',
        'usage',
        'filename',               # Mantener filename también
        'link'
    ]
    
    # Crear dataset final
    fashion_multimedia_df = final_df[multimedia_columns].copy()
    
    # CORREGIDO: Guardar exactamente lo que necesitas
    print("\n Guardando datasets finales...")
    
    # 1. Dataset completo (TODO el dataset)
    complete_output = "datos/fashion_complete_dataset.csv"
    fashion_multimedia_df.to_csv(complete_output, index=False)
    print(f" Dataset COMPLETO guardado en: {complete_output} ({len(fashion_multimedia_df):,} imágenes)")
    
    # 2. Dataset de 100 imágenes (para demos)
    if len(fashion_multimedia_df) >= 100:
        sample_100 = fashion_multimedia_df.head(100)
        demo_output = "datos/fashion_demo_100.csv"
        sample_100.to_csv(demo_output, index=False)
        print(f" Dataset de 100 imágenes guardado en: {demo_output}")
    
    # 3. Dataset textual para SPIMI (opcional)
    textual_columns = [
        'id',
        'productDisplayName',
        'combined_text',
        'gender',
        'masterCategory',
        'subCategory',
        'articleType',
        'baseColour'
    ]
    
    textual_df = final_df[textual_columns].copy()
    textual_output = "datos/fashion_textual.csv"
    textual_df.to_csv(textual_output, index=False)
    print(f" Dataset textual guardado en: {textual_output} (opcional)")
    
    # Verificar archivos creados
    print(f"\n Verificando archivos creados...")
    created_files = [complete_output, demo_output, textual_output]
    
    for file_path in created_files:
        if os.path.exists(file_path):
            df_check = pd.read_csv(file_path)
            valid_images = 0
            
            if 'image_path' in df_check.columns:
                for _, row in df_check.head(3).iterrows():
                    if os.path.exists(row['image_path']):
                        valid_images += 1
                
                print(f"   {file_path}: {len(df_check)} filas, {valid_images}/3 imágenes válidas")
            else:
                print(f"   {file_path}: {len(df_check)} filas (textual)")
    
    # Estadísticas
    print("\n ESTADÍSTICAS DEL DATASET:")
    print("=" * 50)
    print(f"Total de productos: {len(final_df):,}")
    print(f"Géneros: {final_df['gender'].value_counts().to_dict()}")
    print(f"Categorías principales: {final_df['masterCategory'].value_counts().to_dict()}")
    print(f"Años: {final_df['year'].value_counts().sort_index().to_dict()}")
    
    print(f"\n ARCHIVOS FINALES LISTOS:")
    print(f"   Dataset completo:     {complete_output} ({len(fashion_multimedia_df):,} imágenes)")
    print(f"   Dataset de 100:       {demo_output} (100 imágenes)")
    print(f"   Búsqueda textual:     {textual_output} (opcional)")
    
    print(f"\n COMANDOS SQL PARA USAR:")
    print(f"  # Dataset de 100 imágenes (RECOMENDADO para empezar)")
    print(f'  CREATE MULTIMEDIA TABLE fashion FROM FILE "{demo_output}" USING image WITH METHOD sift CLUSTERS 64;')
    print(f"")
    print(f"  # Dataset completo ({len(fashion_multimedia_df):,} imágenes - REQUIERE TIEMPO)")
    print(f'  CREATE MULTIMEDIA TABLE fashion FROM FILE "{complete_output}" USING image WITH METHOD sift CLUSTERS 128;')
    
    print(f"\n RECOMENDACIÓN:")
    print(f"  - Empieza con el dataset de 100 imágenes (procesamiento rápido)")
    print(f"  - Usa el dataset completo solo si necesitas el máximo de datos")
    
    return final_df

def show_dataset_info():
    """Muestra información del dataset creado"""
    dataset_path = "datos/fashion_complete_dataset.csv"
    
    if not os.path.exists(dataset_path):
        print(" Dataset completo no encontrado. Ejecuta create_unified_fashion_dataset() primero")
        return
    
    df = pd.read_csv(dataset_path)
    
    print("\n INFORMACIÓN DEL DATASET FASHION COMPLETO")
    print("=" * 60)
    print(f"Filas: {len(df):,}")
    print(f"Columnas: {len(df.columns)}")
    print(f"Primeras rutas de imagen:")
    
    for i, path in enumerate(df['image_path'].head(5), 1):
        exists = "" if os.path.exists(path) else ""
        filename = os.path.basename(path)
        print(f"  {i}. {exists} {filename}")
    
    print(f"\nCampos textuales disponibles:")
    text_fields = ['productDisplayName', 'combined_text', 'gender', 'masterCategory', 'articleType']
    for field in text_fields:
        if field in df.columns:
            print(f"  - {field}: {df[field].iloc[0][:50]}...")
    
    print(f"\nEjemplo de texto combinado:")
    print(f"  {df['combined_text'].iloc[0]}")
    
    # Info del dataset de 100 también
    demo_path = "datos/fashion_demo_100.csv"
    if os.path.exists(demo_path):
        demo_df = pd.read_csv(demo_path)
        print(f"\n DATASET DE 100 IMÁGENES:")
        print(f"Filas: {len(demo_df)}")
        valid_demo = sum(1 for _, row in demo_df.head(5).iterrows() if os.path.exists(row['image_path']))
        print(f"Imágenes válidas (muestra): {valid_demo}/5")

if __name__ == "__main__":
    print(" Fashion Dataset Processor - VERSIÓN CORREGIDA")
    print("=" * 50)
    
    # Crear dataset unificado
    dataset = create_unified_fashion_dataset()
    
    if dataset is not None:
        # Mostrar información
        show_dataset_info()
        
        print("\n ¡LISTO! Archivos creados con rutas corregidas")
        print(" Ahora puedes usar el sistema multimedia:")
        print("  python main.py")
        print("  → Seleccionar: '3. PARSER SQL MULTIMEDIA'")
        print("  → Usar los comandos SQL mostrados arriba")