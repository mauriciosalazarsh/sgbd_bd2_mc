# multimedia/feature_extractors/image_extractor_batch.py
import numpy as np
import pickle
import os
import gc
from typing import List, Tuple, Optional, Any, Union

# Importaciones seguras para evitar errores
try:
    import cv2
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    cv2 = None

class BatchImageFeatureExtractor:
    def __init__(self, method='sift', batch_size=100):
        """
        Extractor de características optimizado para lotes
        
        Args:
            method: 'sift' únicamente por ahora
            batch_size: tamaño del lote para procesamiento
        """
        self.method = method.lower()
        self.batch_size = batch_size
        self.sift = None
        
        if self.method == 'sift':
            if not CV2_AVAILABLE or cv2 is None:
                raise ImportError("OpenCV no está instalado. Ejecuta: pip install opencv-python")
            
            # Crear detector SIFT con parámetros optimizados
            self.sift = cv2.SIFT_create(
                nfeatures=128,  # Limitar número de keypoints
                contrastThreshold=0.08,  # Aumentar threshold para menos keypoints
                edgeThreshold=15,  # Aumentar para filtrar más
                sigma=1.2  # Reducir sigma para procesamiento más rápido
            )
    
    def extract_sift_features_batch(self, image_paths: List[str], save_every: int = 1000) -> str:
        """
        Extrae características SIFT por lotes y guarda incrementalmente
        
        Args:
            image_paths: lista de rutas de imágenes
            save_every: guardar resultados cada N imágenes
            
        Returns:
            ruta del archivo con características
        """
        total = len(image_paths)
        output_path = "embeddings/temp_features.pkl"
        os.makedirs("embeddings", exist_ok=True)
        
        print(f"\n🖼️  Extrayendo características SIFT en lotes")
        print(f"Total de imágenes: {total}")
        print(f"Tamaño de lote: {self.batch_size}")
        print(f"Guardando cada: {save_every} imágenes")
        
        all_features = []
        processed = 0
        failed = 0
        
        for batch_start in range(0, total, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total)
            batch_paths = image_paths[batch_start:batch_end]
            
            # Procesar lote
            batch_features = []
            for path in batch_paths:
                features = self._extract_single_sift(path)
                if features is not None:
                    batch_features.append((path, features))
                else:
                    failed += 1
                
                processed += 1
                
                # Mostrar progreso
                if processed % 100 == 0 or processed == total:
                    progress = processed / total * 100
                    print(f"\rProgreso: {progress:.1f}% ({processed}/{total}) - Fallos: {failed}", end='', flush=True)
            
            # Agregar al resultado total
            all_features.extend(batch_features)
            
            # Guardar incrementalmente
            if processed % save_every == 0 or processed == total:
                print(f"\n💾 Guardando {len(all_features)} características...")
                with open(output_path, 'wb') as f:
                    pickle.dump(all_features, f)
                
                # Liberar memoria
                gc.collect()
                
                # Opcional: comprimir características antiguas
                if len(all_features) > save_every * 2:
                    # Mantener solo las últimas características en memoria
                    temp_features = all_features[-save_every:]
                    all_features = temp_features
        
        print(f"\n✅ Extracción completada: {processed - failed}/{total} exitosas")
        return output_path
    
    def _extract_single_sift(self, image_path: str) -> Optional[np.ndarray]:
        """Extrae SIFT de una sola imagen con manejo de errores"""
        try:
            # Leer imagen en escala de grises
            img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
            if img is None:
                return None
            
            # Redimensionar si es muy grande para ahorrar memoria
            height, width = img.shape
            max_dim = 500  # Máximo 500x500
            
            if height > max_dim or width > max_dim:
                scale = max_dim / max(height, width)
                new_width = int(width * scale)
                new_height = int(height * scale)
                img = cv2.resize(img, (new_width, new_height))
            
            # Extraer características
            keypoints, descriptors = self.sift.detectAndCompute(img, None)
            
            # Liberar imagen de memoria inmediatamente
            del img
            
            if descriptors is None or len(descriptors) == 0:
                # Retornar descriptor dummy si no hay características
                return np.zeros((1, 128), dtype=np.float32)
            
            # Limitar número de descriptores para ahorrar memoria
            if len(descriptors) > 50:
                # Mantener solo los 50 más fuertes
                descriptors = descriptors[:50]
            
            return descriptors.astype(np.float32)
            
        except Exception as e:
            return None
    
    def process_dataset_in_chunks(self, csv_path: str, image_column: str, 
                                 chunk_size: int = 5000) -> List[str]:
        """
        Procesa un dataset completo en chunks para manejar archivos grandes
        
        Returns:
            Lista de archivos de características generados
        """
        import pandas as pd
        
        print(f"\n📊 Procesando dataset en chunks de {chunk_size} registros")
        
        output_files = []
        chunk_num = 0
        
        # Leer CSV en chunks
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, encoding='latin1'):
            chunk_num += 1
            print(f"\n📦 Procesando chunk {chunk_num}")
            
            # Obtener rutas de imágenes válidas
            image_paths = []
            for path in chunk[image_column]:
                if pd.notna(path) and os.path.exists(path):
                    image_paths.append(path)
            
            if not image_paths:
                print(f"⚠️  Chunk {chunk_num} no tiene imágenes válidas")
                continue
            
            # Procesar chunk
            output_file = f"embeddings/features_chunk_{chunk_num}.pkl"
            features = self.extract_sift_features_batch(image_paths, save_every=1000)
            
            # Renombrar archivo temporal
            if os.path.exists(features):
                os.rename(features, output_file)
                output_files.append(output_file)
            
            # Forzar liberación de memoria
            del chunk
            gc.collect()
            
            print(f"✅ Chunk {chunk_num} completado")
        
        return output_files
    
    @staticmethod
    def merge_feature_files(feature_files: List[str], output_path: str) -> int:
        """Combina múltiples archivos de características en uno solo"""
        print(f"\n🔀 Combinando {len(feature_files)} archivos de características...")
        
        all_features = []
        total_features = 0
        
        for file_path in feature_files:
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    features = pickle.load(f)
                    all_features.extend(features)
                    total_features += len(features)
                
                # Eliminar archivo temporal
                os.remove(file_path)
        
        # Guardar archivo combinado
        with open(output_path, 'wb') as f:
            pickle.dump(all_features, f)
        
        print(f"✅ Combinados {total_features} registros en {output_path}")
        return total_features

# Función de utilidad para uso directo
def extract_features_memory_efficient(csv_path: str, image_column: str, 
                                    output_path: str = "embeddings/features_complete.pkl"):
    """
    Función de alto nivel para extraer características de forma eficiente
    
    Args:
        csv_path: ruta del CSV con datos
        image_column: nombre de la columna con rutas de imágenes
        output_path: donde guardar las características
    """
    extractor = BatchImageFeatureExtractor(method='sift', batch_size=50)
    
    # Procesar en chunks
    chunk_files = extractor.process_dataset_in_chunks(
        csv_path, 
        image_column, 
        chunk_size=5000
    )
    
    # Combinar resultados
    if chunk_files:
        BatchImageFeatureExtractor.merge_feature_files(chunk_files, output_path)
        print(f"\n🎉 Proceso completado! Características guardadas en: {output_path}")
    else:
        print("\n❌ No se procesaron características")

if __name__ == "__main__":
    # Ejemplo de uso
    print("Extractor de características optimizado para memoria")
    print("Uso: extract_features_memory_efficient('datos.csv', 'columna_imagen')")