#!/usr/bin/env python3
"""
Script para arreglar las rutas en el CSV del dataset Fashion
"""

import os
import pandas as pd

def fix_fashion_csv_paths():
    """Arregla las rutas en los CSV del dataset Fashion"""
    print(" Arreglando rutas del dataset Fashion...")
    
    # Rutas de archivos
    csv_files = [
        'datos/fashion_multimedia.csv',
        'datos/fashion_sample_100.csv',
        'datos/fashion_sample_1000.csv',
        'datos/fashion_balanced.csv'
    ]
    
    images_dir = 'datos/fashion-dataset/images'
    
    # Verificar que el directorio de imágenes existe
    if not os.path.exists(images_dir):
        print(f" Directorio de imágenes no encontrado: {images_dir}")
        return False
    
    # Verificar cuántas imágenes hay
    try:
        image_files = [f for f in os.listdir(images_dir) if f.endswith('.jpg')]
        print(f"📷 Imágenes encontradas en {images_dir}: {len(image_files)}")
    except Exception as e:
        print(f" Error leyendo directorio de imágenes: {e}")
        return False
    
    # Procesar cada CSV
    fixed_files = []
    
    for csv_file in csv_files:
        if not os.path.exists(csv_file):
            print(f" CSV no encontrado: {csv_file}")
            continue
            
        try:
            print(f"\n Procesando: {csv_file}")
            
            # Leer CSV
            df = pd.read_csv(csv_file)
            print(f"    Filas: {len(df)}")
            print(f"    Columnas: {list(df.columns)}")
            
            # Identificar columna de archivos
            filename_col = None
            path_col = None
            
            for col in df.columns:
                if 'filename' in col.lower():
                    filename_col = col
                elif 'path' in col.lower() and 'image' in col.lower():
                    path_col = col
            
            if not filename_col:
                print(f"    No se encontró columna 'filename'")
                continue
                
            print(f"    Columna filename: {filename_col}")
            if path_col:
                print(f"    Columna path: {path_col}")
            
            # Mostrar rutas actuales
            print(f"    Rutas actuales (primeras 3):")
            for i in range(min(3, len(df))):
                filename = df.iloc[i][filename_col]
                current_path = df.iloc[i][path_col] if path_col else 'N/A'
                print(f"      {i+1}. filename: {filename}, path: {current_path}")
            
            # Construir rutas correctas
            correct_paths = []
            valid_images = 0
            
            for _, row in df.iterrows():
                filename = row[filename_col]
                if pd.isna(filename):
                    correct_paths.append('')
                    continue
                    
                # Construir ruta completa
                full_path = os.path.join(images_dir, filename)
                correct_paths.append(full_path)
                
                # Verificar si la imagen existe
                if os.path.exists(full_path):
                    valid_images += 1
            
            # Actualizar o crear columna image_path
            df['image_path'] = correct_paths
            
            print(f"    Rutas corregidas: {len(correct_paths)}")
            print(f"   📷 Imágenes válidas: {valid_images}/{len(df)}")
            
            # Mostrar rutas corregidas
            print(f"    Rutas corregidas (primeras 3):")
            for i in range(min(3, len(df))):
                path = df.iloc[i]['image_path']
                exists = "" if os.path.exists(path) else ""
                print(f"      {i+1}. {exists} {path}")
            
            # Crear versión corregida
            fixed_filename = csv_file.replace('.csv', '_fixed.csv')
            df.to_csv(fixed_filename, index=False)
            
            print(f"    Guardado como: {fixed_filename}")
            fixed_files.append((fixed_filename, valid_images, len(df)))
            
            # Si hay suficientes imágenes válidas, crear versión funcional
            if valid_images >= 10:
                # Filtrar solo filas con imágenes válidas
                valid_df = df[df['image_path'].apply(os.path.exists)].copy()
                
                # Crear versión pequeña para pruebas
                small_df = valid_df.head(20)
                small_filename = csv_file.replace('.csv', '_working_20.csv')
                small_df.to_csv(small_filename, index=False)
                
                print(f"    Versión funcional (20 imágenes): {small_filename}")
                fixed_files.append((small_filename, len(small_df), len(small_df)))
            
        except Exception as e:
            print(f"    Error procesando {csv_file}: {e}")
    
    # Resumen
    print(f"\n RESUMEN:")
    print(f"Archivos procesados: {len(fixed_files)}")
    
    for filename, valid, total in fixed_files:
        print(f"   {filename}: {valid}/{total} imágenes válidas")
    
    # Recomendar archivo para usar
    best_file = None
    best_count = 0
    
    for filename, valid, total in fixed_files:
        if valid > best_count:
            best_count = valid
            best_file = filename
    
    if best_file:
        print(f"\n ARCHIVO RECOMENDADO PARA USAR:")
        print(f"   {best_file} ({best_count} imágenes válidas)")
        print(f"\n Comando SQL:")
        print(f'   CREATE MULTIMEDIA TABLE fashion FROM FILE "{best_file}" USING image WITH METHOD sift CLUSTERS 32;')
    
    return len(fixed_files) > 0

def verify_image_samples():
    """Verifica algunas imágenes de muestra"""
    print(f"\n Verificando imágenes de muestra...")
    
    images_dir = 'datos/fashion-dataset/images'
    
    try:
        all_images = [f for f in os.listdir(images_dir) if f.endswith('.jpg')]
        sample_images = all_images[:5]  # Primeras 5 imágenes
        
        print(f"📷 Imágenes de muestra:")
        for img in sample_images:
            img_path = os.path.join(images_dir, img)
            size = os.path.getsize(img_path)
            print(f"   {img} ({size:,} bytes)")
            
        return True
        
    except Exception as e:
        print(f" Error verificando imágenes: {e}")
        return False

if __name__ == "__main__":
    print(" ARREGLANDO DATASET FASHION")
    print("=" * 50)
    
    # Verificar imágenes
    verify_image_samples()
    
    # Arreglar CSVs
    success = fix_fashion_csv_paths()
    
    if success:
        print(f"\n ¡Dataset Fashion arreglado exitosamente!")
        print(f"Ahora puedes usar el sistema multimedia con los archivos _fixed.csv o _working_20.csv")
    else:
        print(f"\n No se pudo arreglar el dataset")