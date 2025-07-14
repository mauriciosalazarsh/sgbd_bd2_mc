#!/usr/bin/env python3
"""
Script FINAL para crear dataset FMA con 100 archivos y dataset completo
"""

import os
import pandas as pd
import glob
from pathlib import Path

def scan_fma_audio_files():
    """Escanea y cataloga todos los archivos de audio del dataset FMA Medium"""
    print("🎵 Escaneando archivos de audio FMA Medium...")
    
    fma_dir = "datos/fma_medium"
    if not os.path.exists(fma_dir):
        print(f" Directorio FMA no encontrado: {fma_dir}")
        return None
    
    # Buscar todos los archivos .mp3 en los subdirectorios
    audio_files = []
    
    # Escanear directorios numerados (000, 001, etc.)
    subdirs = [d for d in os.listdir(fma_dir) if os.path.isdir(os.path.join(fma_dir, d)) and d.isdigit()]
    subdirs.sort()
    
    for subdir in subdirs:
        subdir_path = os.path.join(fma_dir, subdir)
        mp3_files = glob.glob(os.path.join(subdir_path, "*.mp3"))
        
        for mp3_file in mp3_files:
            # Extraer información del archivo
            filename = os.path.basename(mp3_file)
            track_id = filename.replace('.mp3', '')
            
            # Obtener tamaño del archivo
            try:
                file_size = os.path.getsize(mp3_file)
            except:
                file_size = 0
            
            audio_files.append({
                'track_id': track_id,
                'filename': filename,
                'audio_path': mp3_file,  # CORREGIDO: usar audio_path
                'subdirectory': subdir,
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2)
            })
    
    print(f" Archivos de audio encontrados: {len(audio_files)}")
    return audio_files

def create_audio_metadata(audio_files):
    """Crea metadatos sintéticos para los archivos de audio"""
    print(" Creando metadatos sintéticos...")
    
    # Géneros musicales comunes
    genres = [
        'Rock', 'Pop', 'Jazz', 'Classical', 'Electronic', 'Hip-Hop', 
        'Country', 'Blues', 'Folk', 'Reggae', 'Metal', 'Punk',
        'Indie', 'Alternative', 'R&B', 'Soul', 'Funk', 'Ambient'
    ]
    
    # Artistas sintéticos
    artists = [
        'The Audio Artists', 'Digital Harmony', 'Sound Collective', 
        'Electronic Dreams', 'Acoustic Soul', 'Rhythm Masters',
        'Melody Makers', 'Beat Generation', 'Sound Waves', 'Audio Vision',
        'Music Box', 'Sound Factory', 'Digital Music', 'Audio Craft',
        'Sound Studio', 'Music Lab', 'Audio Experiments', 'Sound Design'
    ]
    
    import random
    random.seed(42)  # Para reproducibilidad
    
    enhanced_files = []
    for i, audio_file in enumerate(audio_files):
        # Generar metadatos sintéticos
        genre = random.choice(genres)
        artist = random.choice(artists)
        
        # Crear título basado en el track_id
        track_num = i + 1
        title = f"Track {track_num:04d}"
        
        # Duración estimada (entre 2-6 minutos)
        duration_seconds = random.randint(120, 360)
        duration_minutes = f"{duration_seconds // 60}:{duration_seconds % 60:02d}"
        
        # Año aleatorio
        year = random.randint(2000, 2023)
        
        # Rating aleatorio
        rating = round(random.uniform(1.0, 5.0), 1)
        
        enhanced_file = {
            **audio_file,
            'title': title,
            'artist': artist,
            'genre': genre,
            'duration_seconds': duration_seconds,
            'duration': duration_minutes,
            'year': year,
            'rating': rating,
            'album': f"{artist} - Collection",
            'combined_text': f"{title} {artist} {genre} {year}"
        }
        
        enhanced_files.append(enhanced_file)
    
    return enhanced_files

def create_unified_fma_dataset():
    """Crea datasets FMA: 100 archivos y dataset completo"""
    print("🎵 CREANDO DATASETS FMA FINAL")
    print("=" * 60)
    
    # 1. Escanear archivos de audio
    audio_files = scan_fma_audio_files()
    if not audio_files:
        return False
    
    # 2. Crear metadatos sintéticos
    enhanced_files = create_audio_metadata(audio_files)
    
    # 3. Crear DataFrame principal
    df = pd.DataFrame(enhanced_files)
    print(f" Dataset principal creado: {len(df)} registros")
    
    # 4. Verificar archivos (muestra)
    print(f" Verificando archivos de muestra...")
    valid_samples = 0
    for _, row in df.head(5).iterrows():
        if os.path.exists(row['audio_path']):
            valid_samples += 1
            print(f"   {row['filename']}")
        else:
            print(f"   {row['filename']} (no existe)")
    
    if valid_samples == 0:
        print(" No se encontraron archivos válidos")
        return False
    
    print(f" {valid_samples}/5 archivos de muestra válidos")
    
    # 5. GENERAR DATASETS FINALES
    print(f"\n Generando datasets finales...")
    
    # Columnas para sistema multimedia
    multimedia_columns = [
        'track_id',
        'filename',
        'audio_path',          # IMPORTANTE: Esta es la columna que detecta el parser
        'title',
        'artist', 
        'genre',
        'duration_seconds',
        'duration',
        'year',
        'rating',
        'album',
        'combined_text',
        'subdirectory',
        'file_size_mb'
    ]
    
    # Dataset final con todas las columnas
    fma_df = df[multimedia_columns].copy()
    
    # A. Dataset de 100 archivos (PARA DEMOS)
    fma_100 = fma_df.head(100)
    output_100 = "datos/fma_demo_100.csv"
    fma_100.to_csv(output_100, index=False)
    print(f" Dataset de 100 archivos: {output_100}")
    
    # B. Dataset completo (TODO EL FMA MEDIUM)
    complete_output = "datos/fma_complete_dataset.csv"
    fma_df.to_csv(complete_output, index=False)
    print(f" Dataset completo: {complete_output} ({len(fma_df):,} archivos)")
    
    # C. Dataset textual para SPIMI (opcional)
    textual_columns = [
        'track_id',
        'title',
        'artist',
        'genre',
        'album',
        'combined_text',
        'year'
    ]
    
    textual_df = df[textual_columns].copy()
    textual_output = "datos/fma_textual.csv"
    textual_df.to_csv(textual_output, index=False)
    print(f" Dataset textual: {textual_output} (opcional)")
    
    # 6. Verificar archivos creados
    print(f"\n Verificando archivos creados...")
    
    datasets_to_check = [
        (output_100, "100 archivos"),
        (complete_output, f"{len(fma_df):,} archivos"),
        (textual_output, "textual")
    ]
    
    for file_path, description in datasets_to_check:
        if os.path.exists(file_path):
            df_check = pd.read_csv(file_path)
            
            if 'audio_path' in df_check.columns:
                # Verificar archivos de audio
                valid_audio = 0
                for _, row in df_check.head(3).iterrows():
                    if os.path.exists(row['audio_path']):
                        valid_audio += 1
                
                print(f"   {file_path}: {len(df_check)} filas, {valid_audio}/3 archivos válidos")
            else:
                print(f"   {file_path}: {len(df_check)} filas (textual)")
    
    # 7. Estadísticas del dataset
    print(f"\n ESTADÍSTICAS DEL DATASET FMA:")
    print("=" * 50)
    print(f"Total de tracks: {len(df):,}")
    print(f"Géneros: {df['genre'].nunique()} únicos")
    genre_counts = df['genre'].value_counts().head(5).to_dict()
    print(f"  Top 5: {genre_counts}")
    print(f"Artistas: {df['artist'].nunique()} únicos")
    print(f"Años: {df['year'].min()} - {df['year'].max()}")
    print(f"Duración promedio: {df['duration_seconds'].mean()/60:.1f} minutos")
    print(f"Tamaño promedio: {df['file_size_mb'].mean():.2f} MB")
    
    # 8. Comandos SQL recomendados
    print(f"\n ARCHIVOS FINALES LISTOS:")
    print(f"  🎵 Dataset de 100:      {output_100} (100 archivos)")
    print(f"  🎵 Dataset completo:    {complete_output} ({len(fma_df):,} archivos)")
    print(f"   Búsqueda textual:    {textual_output} (opcional)")
    
    print(f"\n COMANDOS SQL PARA USAR:")
    print(f"  # Dataset de 100 archivos (RECOMENDADO para empezar)")
    print(f'  CREATE MULTIMEDIA TABLE fma FROM FILE "{output_100}" USING audio WITH METHOD mfcc CLUSTERS 64;')
    print(f"")
    print(f"  # Dataset completo ({len(fma_df):,} archivos - REQUIERE MUCHO TIEMPO)")
    print(f'  CREATE MULTIMEDIA TABLE fma FROM FILE "{complete_output}" USING audio WITH METHOD mfcc CLUSTERS 128;')
    
    print(f"\n RECOMENDACIÓN:")
    print(f"  - Empieza con 100 archivos (procesamiento: ~2-3 minutos)")
    print(f"  - Dataset completo puede tomar 1+ hora con {len(fma_df):,} archivos")
    
    print(f"\n🎵 ARCHIVOS DE CONSULTA DISPONIBLES:")
    print(f"  - datos/fma_medium/000/000762.mp3")
    print(f"  - datos/fma_medium/000/000776.mp3") 
    print(f"  - datos/fma_medium/000/000010.mp3")
    
    return True

def show_fma_info():
    """Muestra información de los datasets FMA creados"""
    
    demo_path = "datos/fma_demo_100.csv"
    complete_path = "datos/fma_complete_dataset.csv"
    
    print("\n INFORMACIÓN DE DATASETS FMA")
    print("=" * 50)
    
    # Dataset de 100
    if os.path.exists(demo_path):
        df_demo = pd.read_csv(demo_path)
        print(f"🎵 Dataset de 100 archivos:")
        print(f"  Filas: {len(df_demo)}")
        print(f"  Géneros: {df_demo['genre'].nunique()}")
        
        # Verificar archivos
        valid_demo = 0
        for _, row in df_demo.head(5).iterrows():
            if os.path.exists(row['audio_path']):
                valid_demo += 1
        print(f"  Archivos válidos (muestra): {valid_demo}/5")
    
    # Dataset completo
    if os.path.exists(complete_path):
        df_complete = pd.read_csv(complete_path)
        print(f"\n🎵 Dataset completo:")
        print(f"  Filas: {len(df_complete):,}")
        print(f"  Géneros: {df_complete['genre'].nunique()}")
        print(f"  Artistas: {df_complete['artist'].nunique()}")
        
        # Verificar archivos
        valid_complete = 0
        for _, row in df_complete.head(5).iterrows():
            if os.path.exists(row['audio_path']):
                valid_complete += 1
        print(f"  Archivos válidos (muestra): {valid_complete}/5")
    
    # Dataset textual
    textual_path = "datos/fma_textual.csv"
    if os.path.exists(textual_path):
        df_textual = pd.read_csv(textual_path)
        print(f"\n Dataset textual:")
        print(f"  Filas: {len(df_textual):,}")
        print(f"  Ejemplo: {df_textual['combined_text'].iloc[0]}")

if __name__ == "__main__":
    print("🎵 FMA Dataset Processor - VERSIÓN FINAL")
    print("=" * 50)
    
    # Crear datasets
    success = create_unified_fma_dataset()
    
    if success:
        # Mostrar información
        show_fma_info()
        
        print("\n ¡DATASETS FMA LISTOS!")
        print(" Para probar el sistema:")
        print("  python main.py")
        print("  → 3.  PARSER SQL MULTIMEDIA")
        print("  → 3.  Crear tabla multimedia")
        print("  → Usar los comandos SQL mostrados arriba")
    else:
        print("\n No se pudieron crear los datasets FMA")