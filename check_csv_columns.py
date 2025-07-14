#!/usr/bin/env python3
"""
Script rápido para verificar las columnas del CSV FMA
"""

import pandas as pd

def check_csv_structure():
    csv_file = "datos/fma_working_minimal.csv"
    
    print(f" Verificando estructura de: {csv_file}")
    
    df = pd.read_csv(csv_file)
    
    print(f" Filas: {len(df)}")
    print(f" Columnas: {list(df.columns)}")
    
    # Mostrar primeras filas
    print(f"\n Primeras 3 filas:")
    for i in range(min(3, len(df))):
        row = df.iloc[i]
        print(f"\nFila {i+1}:")
        print(f"  filename: {row['filename']}")
        print(f"  audio_path: {row['audio_path']}")
        
        # Verificar si existe el archivo
        import os
        exists = os.path.exists(row['audio_path'])
        print(f"  ¿Existe archivo?: {'' if exists else ''}")

if __name__ == "__main__":
    check_csv_structure()