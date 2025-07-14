#!/usr/bin/env python3
"""
Script to fix the audio similarity bug by updating the codebook builder
"""

import os
import shutil

def fix_audio_similarity():
    """Apply the fix for audio similarity"""
    
    # Backup original file
    original_file = "multimedia/codebook/builder.py"
    backup_file = "multimedia/codebook/builder_original.py"
    fixed_file = "multimedia/codebook/builder_fixed.py"
    
    print("Fixing audio similarity bug...")
    
    # Create backup
    if os.path.exists(original_file) and not os.path.exists(backup_file):
        shutil.copy2(original_file, backup_file)
        print(f"✓ Created backup: {backup_file}")
    
    # Copy fixed version
    if os.path.exists(fixed_file):
        shutil.copy2(fixed_file, original_file)
        print(f"✓ Applied fix from: {fixed_file}")
    else:
        print(f" Fixed file not found: {fixed_file}")
        return False
    
    # Update multimedia_engine.py to pass feature_type to CodebookBuilder
    update_multimedia_engine()
    
    print("\n Fix applied successfully!")
    print("\nChanges made:")
    print("1. CodebookBuilder now handles audio features differently")
    print("2. Audio features are used directly as histograms (no clustering)")
    print("3. This prevents the 100% similarity bug for audio files")
    print("\nYou may need to rebuild your audio indices for the fix to take effect.")
    
    return True

def update_multimedia_engine():
    """Update multimedia engine to pass feature_type parameter"""
    engine_file = "multimedia/multimedia_engine.py"
    
    # Read the file
    with open(engine_file, 'r') as f:
        content = f.read()
    
    # Replace the CodebookBuilder initialization
    old_line = "self.codebook_builder = CodebookBuilder(n_clusters=n_clusters)"
    new_line = "self.codebook_builder = CodebookBuilder(n_clusters=n_clusters, feature_type=media_type)"
    
    if old_line in content:
        content = content.replace(old_line, new_line)
        
        # Write back
        with open(engine_file, 'w') as f:
            f.write(content)
        
        print(f"✓ Updated {engine_file} to pass feature_type")
    else:
        print(f"  Could not find line to update in {engine_file}")
        print("  You may need to manually update the CodebookBuilder initialization")
        print(f"  Change: {old_line}")
        print(f"  To: {new_line}")

if __name__ == "__main__":
    fix_audio_similarity()