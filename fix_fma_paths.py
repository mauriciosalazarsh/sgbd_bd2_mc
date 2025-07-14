#!/usr/bin/env python3
"""
Script para arreglar las rutas en los CSV del dataset FMA
"""

import os
import pandas as pd
import glob

def check_fma_structure():
    """Verifica la estructura del dataset FMA"""
    print(" Verificando estructura del dataset FMA...")
    
    fma_dir = "datos/fma_medium"
    if not os.path.exists(fma_dir):
        print(f" Directorio FMA no encontrado: {fma_dir}")
        return False
    
    # Verificar subdirectorios
    subdirs = [d for d in os.listdir(fma_dir) if os.path.isdir(os.path.join(fma_dir, d)) and d.isdigit()]
    print(f" Subdirectorios encontrados: {len(subdirs)}")
    
    # Verificar algunos archivos
    sample_files = []
    for subdir in subdirs[:3]:  # Solo primeros 3 directorios
        subdir_path = os.path.join(fma_dir, subdir)
        mp3_files = glob.glob(os.path.join(subdir_path, "*.mp3"))
        sample_files.extend(mp3_files[:2])  # 2 archivos por directorio
        print(f"   {subdir}/: {len(mp3_files)} archivos .mp3")
    
    print(f"\n🎵 Archivos de muestra encontrados:")
    for file_path in sample_files:
        filename = os.path.basename(file_path)
        size = os.path.getsize(file_path)
        print(f"   {filename} ({size:,} bytes)")
    
    return len(sample_files) > 0

def fix_fma_csv_paths():
    """Arregla las rutas en los CSV del dataset FMA"""
    print("\n Arreglando rutas del dataset FMA...")
    
    # Archivos CSV a procesar
    csv_files = [
        'datos/fma_multimedia.csv',
        'datos/fma_sample_50.csv',
        'datos/fma_sample_200.csv',
        'datos/fma_balanced.csv'
    ]
    
    fma_dir = 'datos/fma_medium'
    
    # Verificar que el directorio de audio existe
    if not os.path.exists(fma_dir):
        print(f" Directorio de audio no encontrado: {fma_dir}")
        return False
    
    # Crear mapeo de filename -> ruta completa
    print(" Creando mapeo de archivos...")
    file_mapping = {}
    
    subdirs = [d for d in os.listdir(fma_dir) if os.path.isdir(os.path.join(fma_dir, d)) and d.isdigit()]
    for subdir in subdirs:
        subdir_path = os.path.join(fma_dir, subdir)
        mp3_files = glob.glob(os.path.join(subdir_path, "*.mp3"))
        
        for mp3_file in mp3_files:
            filename = os.path.basename(mp3_file)
            file_mapping[filename] = mp3_file
    
    print(f" Mapeo creado: {len(file_mapping)} archivos")
    
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
                elif 'path' in col.lower() and 'audio' in col.lower():
                    path_col = col
            
            if not filename_col:
                print(f"    No se encontró columna 'filename'")
                continue
                
            print(f"    Columna filename: {filename_col}")
            if path_col:
                print(f"    Columna path existente: {path_col}")
            
            # Construir rutas correctas
            correct_paths = []
            valid_audio = 0
            
            for _, row in df.iterrows():
                filename = row[filename_col]
                if pd.isna(filename):
                    correct_paths.append('')
                    continue
                
                # Buscar archivo en el mapeo
                if filename in file_mapping:
                    full_path = file_mapping[filename]
                    correct_paths.append(full_path)
                    valid_audio += 1
                else:
                    # Intentar buscar agregando .mp3 si no lo tiene
                    if not filename.endswith('.mp3'):
                        filename_with_ext = filename + '.mp3'
                        if filename_with_ext in file_mapping:
                            full_path = file_mapping[filename_with_ext]
                            correct_paths.append(full_path)
                            valid_audio += 1
                        else:
                            correct_paths.append('')
                    else:
                        correct_paths.append('')
            
            # Actualizar o crear columna audio_path
            df['audio_path'] = correct_paths
            
            print(f"    Rutas corregidas: {len(correct_paths)}")
            print(f"   🎵 Archivos de audio válidos: {valid_audio}/{len(df)}")
            
            # Mostrar rutas corregidas
            print(f"    Rutas corregidas (primeras 3):")
            for i in range(min(3, len(df))):
                path = df.iloc[i]['audio_path']
                if path:
                    exists = "" if os.path.exists(path) else ""
                    filename = os.path.basename(path)
                    print(f"      {i+1}. {exists} {filename}")
                else:
                    print(f"      {i+1}.  Sin ruta")
            
            # Crear versión corregida
            fixed_filename = csv_file.replace('.csv', '_fixed.csv')
            df.to_csv(fixed_filename, index=False)
            
            print(f"    Guardado como: {fixed_filename}")
            fixed_files.append((fixed_filename, valid_audio, len(df)))
            
            # Si hay suficientes archivos válidos, crear versión funcional
            if valid_audio >= 10:
                # Filtrar solo filas con archivos válidos
                valid_df = df[df['audio_path'].apply(lambda x: x and os.path.exists(x))].copy()
                
                # Crear versión pequeña para pruebas
                small_df = valid_df.head(20)
                small_filename = csv_file.replace('.csv', '_working_20.csv')
                small_df.to_csv(small_filename, index=False)
                
                print(f"    Versión funcional (20 archivos): {small_filename}")
                fixed_files.append((small_filename, len(small_df), len(small_df)))
            
        except Exception as e:
            print(f"    Error procesando {csv_file}: {e}")
    
    # Resumen
    print(f"\n RESUMEN:")
    print(f"Archivos procesados: {len(fixed_files)}")
    
    for filename, valid, total in fixed_files:
        print(f"   {filename}: {valid}/{total} archivos válidos")
    
    # Recomendar archivo para usar
    best_file = None
    best_count = 0
    
    for filename, valid, total in fixed_files:
        if valid > best_count:
            best_count = valid
            best_file = filename
    
    if best_file:
        print(f"\n ARCHIVO RECOMENDADO PARA USAR:")
        print(f"   {best_file} ({best_count} archivos válidos)")
        print(f"\n Comando SQL:")
        print(f'   CREATE MULTIMEDIA TABLE fma FROM FILE "{best_file}" USING audio WITH METHOD mfcc CLUSTERS 32;')
    
    return len(fixed_files) > 0

def verify_audio_samples():
    """Verifica algunas muestras de audio"""
    print(f"\n Verificando archivos de audio de muestra...")
    
    fma_dir = 'datos/fma_medium'
    
    try:
        # Buscar algunos archivos específicos
        sample_paths = [
            os.path.join(fma_dir, '000', '000762.mp3'),
            os.path.join(fma_dir, '000', '000776.mp3'),
            os.path.join(fma_dir, '001', '002230.mp3'),
            os.path.join(fma_dir, '002', '005623.mp3'),
        ]
        
        found_files = []
        for path in sample_paths:
            if os.path.exists(path):
                size = os.path.getsize(path)
                print(f"   {os.path.basename(path)} ({size:,} bytes)")
                found_files.append(path)
            else:
                print(f"   {os.path.basename(path)} (no encontrado)")
        
        if found_files:
            print(f"\n🎵 Archivos encontrados para probar:")
            for path in found_files[:2]:
                print(f"   {path}")
        
        return len(found_files) > 0
        
    except Exception as e:
        print(f" Error verificando archivos: {e}")
        return False

def create_minimal_working_dataset():
    """Crea un dataset mínimo funcional"""
    print(f"\n Creando dataset mínimo funcional...")
    
    fma_dir = 'datos/fma_medium'
    
    # Buscar archivos reales
    audio_files = []
    subdirs = [d for d in os.listdir(fma_dir) if os.path.isdir(os.path.join(fma_dir, d)) and d.isdigit()]
    
    for subdir in sorted(subdirs)[:3]:  # Solo primeros 3 directorios
        subdir_path = os.path.join(fma_dir, subdir)
        mp3_files = glob.glob(os.path.join(subdir_path, "*.mp3"))
        
        for mp3_file in mp3_files[:10]:  # Máximo 10 por directorio
            filename = os.path.basename(mp3_file)
            track_id = filename.replace('.mp3', '')
            
            audio_files.append({
                'track_id': track_id,
                'filename': filename,
                'audio_path': mp3_file,
                'title': f'Track {track_id}',
                'artist': 'Sample Artist',
                'genre': 'Electronic',
                'year': 2020,
                'duration': '3:30'
            })
            
            if len(audio_files) >= 30:  # Máximo 30 archivos
                break
        
        if len(audio_files) >= 30:
            break
    
    if audio_files:
        df = pd.DataFrame(audio_files)
        output_path = "datos/fma_working_minimal.csv"
        df.to_csv(output_path, index=False)
        
        print(f" Dataset mínimo creado: {output_path}")
        print(f" {len(df)} archivos de audio válidos")
        
        # Verificar archivos
        valid_count = 0
        for _, row in df.iterrows():
            if os.path.exists(row['audio_path']):
                valid_count += 1
        
        print(f" {valid_count}/{len(df)} archivos verificados")
        
        if valid_count >= 10:
            print(f"\n USAR ESTE ARCHIVO:")
            print(f'   CREATE MULTIMEDIA TABLE fma FROM FILE "{output_path}" USING audio WITH METHOD mfcc CLUSTERS 16;')
        
        return True
    
    return False

if __name__ == "__main__":
    print("🎵 ARREGLANDO DATASET FMA MEDIUM")
    print("=" * 50)
    
    # Verificar estructura
    if not check_fma_structure():
        print(" No se pudo verificar la estructura del FMA")
        exit(1)
    
    # Verificar muestras
    verify_audio_samples()
    
    # Arreglar CSVs existentes
    fix_fma_csv_paths()
    
    # Crear dataset mínimo funcional
    create_minimal_working_dataset()
    
    print(f"\n ¡Proceso completado!")
    print(f"Ahora puedes usar el sistema multimedia con los archivos corregidos")