#!/usr/bin/env python3
import csv

# Verificar las primeras líneas del dataset
csv_file = "datos/spotify_songs.csv"

try:
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        
        print(f"📋 HEADERS ENCONTRADOS ({len(headers)} total):")
        for i, header in enumerate(headers):
            print(f"   {i:2d}. {header}")
        
        print(f"\n🔍 CAMPOS IMPORTANTES:")
        important_fields = ['track_name', 'track_artist', 'lyrics', 'name', 'artists']
        for field in important_fields:
            if field in headers:
                idx = headers.index(field)
                print(f"   ✅ {field} (índice {idx})")
            else:
                print(f"   ❌ {field} NO ENCONTRADO")
        
        print(f"\n📄 PRIMERA FILA DE DATOS:")
        first_row = next(reader, [])
        for i, header in enumerate(headers[:10]):  # Solo primeros 10 campos
            value = first_row[i] if i < len(first_row) else "N/A"
            print(f"   {header}: {value[:50]}{'...' if len(value) > 50 else ''}")
            
except Exception as e:
    print(f"❌ Error: {e}")