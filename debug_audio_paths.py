#!/usr/bin/env python3
"""Debug script to identify audio path inconsistencies"""

import pickle
import pandas as pd
import os

def main():
    # Load the audio histograms to get indexed paths
    print("Loading audio index...")
    with open('multimedia_data/fma_audio_histograms.pkl', 'rb') as f:
        histogram_data = pickle.load(f)
    
    indexed_paths = [path for path, _ in histogram_data]
    print(f"Total indexed files: {len(indexed_paths)}")
    
    # Load the CSV metadata
    print("\nLoading CSV metadata...")
    df = pd.read_csv('datos/fma_demo_100.csv')
    print(f"Total CSV records: {len(df)}")
    
    # Create lookup maps
    csv_paths = set(df['audio_path'].tolist())
    csv_by_filename = {os.path.basename(path): path for path in csv_paths}
    
    # Check for mismatches
    print("\nChecking for mismatches...")
    
    # 1. Indexed files not in CSV
    not_in_csv = []
    for idx_path in indexed_paths:
        if idx_path not in csv_paths:
            # Try matching by filename
            basename = os.path.basename(idx_path)
            if basename not in csv_by_filename:
                not_in_csv.append(idx_path)
    
    if not_in_csv:
        print(f"\n❌ {len(not_in_csv)} indexed files NOT in CSV:")
        for path in not_in_csv[:5]:
            print(f"   - {path}")
    else:
        print("\n✓ All indexed files found in CSV")
    
    # 2. Check path format consistency
    print("\nPath format analysis:")
    path_formats = {}
    for path in indexed_paths[:100]:  # Check first 100
        parts = path.split('/')
        format_key = f"{len(parts)} parts: {'/'.join(['...' if i > 0 and i < len(parts)-1 else p for i, p in enumerate(parts)])}"
        path_formats[format_key] = path_formats.get(format_key, 0) + 1
    
    for fmt, count in path_formats.items():
        print(f"   {fmt}: {count} files")
    
    # 3. Check if all indexed files exist on disk
    print("\nChecking file existence...")
    missing_files = []
    for path in indexed_paths:
        if not os.path.exists(path):
            missing_files.append(path)
    
    if missing_files:
        print(f"❌ {len(missing_files)} files missing from disk:")
        for path in missing_files[:5]:
            print(f"   - {path}")
    else:
        print("✓ All indexed files exist on disk")
    
    # 4. Sample comparison
    print("\nSample path comparison (first 5 entries):")
    print("Index Path | CSV Path | Match?")
    print("-" * 70)
    
    for i, (idx_path, _) in enumerate(histogram_data[:5]):
        basename = os.path.basename(idx_path)
        csv_match = df[df['filename'] == basename]
        
        if not csv_match.empty:
            csv_path = csv_match.iloc[0]['audio_path']
            match = "✓" if idx_path == csv_path else "✗"
            print(f"{idx_path} | {csv_path} | {match}")
        else:
            print(f"{idx_path} | NOT FOUND IN CSV | ✗")

if __name__ == "__main__":
    main()