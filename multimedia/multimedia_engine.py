# multimedia/multimedia_engine.py - Versión corregida
import os
import time
import pandas as pd
from typing import List, Tuple, Optional, Dict, Any
from .feature_extractors.image_extractor import ImageFeatureExtractor
from .feature_extractors.audio_extractor import AudioFeatureExtractor
from .codebook.builder import CodebookBuilder
from .search.knn_sequential import KNNSequential
from .search.knn_inverted import KNNInvertedIndex

class MultimediaEngine:
    def __init__(self, media_type='image', feature_method='sift', n_clusters=256):
        """
        Motor principal para búsqueda multimedia
        
        Args:
            media_type: 'image' o 'audio'
            feature_method: método de extracción de características
            n_clusters: número de clusters para el codebook
        """
        self.media_type = media_type.lower()
        self.feature_method = feature_method.lower()
        self.n_clusters = n_clusters
        
        # Inicializar componentes
        if self.media_type == 'image':
            self.feature_extractor = ImageFeatureExtractor(method=feature_method)
        elif self.media_type == 'audio':
            self.feature_extractor = AudioFeatureExtractor(method=feature_method)
        else:
            raise ValueError("media_type debe ser 'image' o 'audio'")
        
        self.codebook_builder = CodebookBuilder(n_clusters=n_clusters, feature_type=media_type)
        self.codebook_builder.method = feature_method  # Pass the method to codebook builder
        self.knn_sequential = KNNSequential(use_tfidf=True)
        self.knn_inverted = KNNInvertedIndex(use_tfidf=True)
        
        # Estado del sistema
        self.features_data = []
        self.histograms_data = []
        self.is_built = False
        
    def extract_features_from_paths(self, file_paths: List[str], 
                                   save_features: bool = True, 
                                   features_path: str = "embeddings/features.pkl") -> List[Tuple[str, Any]]:
        """
        Extrae características de una lista de archivos
        
        Args:
            file_paths: rutas de archivos multimedia
            save_features: guardar características extraídas
            features_path: ruta para guardar características
            
        Returns:
            lista de (file_path, features)
        """
        print(f"Extrayendo características {self.feature_method} de {len(file_paths)} archivos...")
        
        features_data = self.feature_extractor.extract_features_batch(file_paths)
        
        if save_features:
            self.feature_extractor.save_features(features_data, features_path)
        
        self.features_data = features_data
        print(f"Características extraídas: {len(features_data)} archivos")
        
        return features_data
    
    def extract_features_from_dataframe(self, df: pd.DataFrame, 
                                       path_column: str,
                                       base_path: str = "",
                                       save_features: bool = True,
                                       features_path: str = "embeddings/features.pkl") -> List[Tuple[str, Any]]:
        """
        Extrae características de archivos especificados en un DataFrame
        
        Args:
            df: DataFrame con información de archivos
            path_column: nombre de la columna que contiene las rutas
            base_path: ruta base para los archivos
            save_features: guardar características extraídas
            features_path: ruta para guardar características
            
        Returns:
            lista de (file_path, features)
        """
        # Construir rutas completas
        file_paths = []
        for path in df[path_column]:
            full_path = os.path.join(base_path, path) if base_path else path
            if os.path.exists(full_path):
                file_paths.append(full_path)
            else:
                print(f"Archivo no encontrado: {full_path}")
        
        return self.extract_features_from_paths(file_paths, save_features, features_path)
    
    def build_codebook(self, features_data: Optional[List[Tuple[str, Any]]] = None,
                      save_codebook: bool = True,
                      codebook_path: str = "embeddings/codebook.pkl") -> Any:
        """
        Construye el diccionario visual/acústico
        
        Args:
            features_data: datos de características (usa self.features_data si es None)
            save_codebook: guardar codebook construido
            codebook_path: ruta para guardar codebook
            
        Returns:
            codebook construido
        """
        if features_data is None:
            features_data = self.features_data
        
        if not features_data:
            raise ValueError("No hay características disponibles para construir el codebook")
        
        print(f"Construyendo codebook con {self.n_clusters} clusters...")
        codebook = self.codebook_builder.build_codebook(
            features_data, 
            save_path=codebook_path if save_codebook else None
        )
        
        return codebook
    
    def create_histograms(self, features_data: Optional[List[Tuple[str, Any]]] = None,
                         save_histograms: bool = True,
                         histograms_path: str = "embeddings/histograms.pkl") -> List[Tuple[str, Any]]:
        """
        Crea histogramas de Bag of Words para todos los objetos
        
        Args:
            features_data: datos de características (usa self.features_data si es None)
            save_histograms: guardar histogramas
            histograms_path: ruta para guardar histogramas
            
        Returns:
            lista de (file_path, histogram)
        """
        if features_data is None:
            features_data = self.features_data
        
        if not self.codebook_builder.is_fitted:
            raise ValueError("El codebook debe construirse antes de crear histogramas")
        
        print("Creando histogramas de Bag of Words...")
        histograms_data = self.codebook_builder.create_histograms_batch(features_data)
        
        if save_histograms:
            import pickle
            with open(histograms_path, 'wb') as f:
                pickle.dump(histograms_data, f)
        
        self.histograms_data = histograms_data
        print(f"Histogramas creados: {len(histograms_data)} objetos")
        
        return histograms_data
    
    def build_search_indices(self, histograms_data: Optional[List[Tuple[str, Any]]] = None):
        """
        Construye los índices de búsqueda (secuencial e invertido)
        
        Args:
            histograms_data: datos de histogramas (usa self.histograms_data si es None)
        """
        if histograms_data is None:
            histograms_data = self.histograms_data
        
        if not histograms_data:
            raise ValueError("No hay histogramas disponibles para construir los índices")
        
        print("Construyendo índices de búsqueda...")
        
        # Construir índice secuencial
        self.knn_sequential.build_database(histograms_data)
        
        # Construir índice invertido
        self.knn_inverted.build_index(histograms_data)
        
        self.is_built = True
        print("Índices de búsqueda construidos exitosamente")
    
    def search_similar(self, query_path: str, k: int = 10, 
                      method: str = 'inverted') -> List[Tuple[str, float]]:
        """
        Busca objetos similares a un archivo de consulta
        
        Args:
            query_path: ruta del archivo de consulta
            k: número de resultados más similares
            method: 'sequential' o 'inverted'
            
        Returns:
            lista de (file_path, similarity_score)
        """
        if not self.is_built:
            raise ValueError("Los índices deben construirse antes de realizar búsquedas")
        
        # Extraer características del query
        query_features = self.feature_extractor.extract_features(query_path)
        if query_features is None:
            error_msg = f"No se pudieron extraer características de: {query_path}. "
            if self.media_type == 'audio':
                error_msg += "El archivo de audio puede estar corrupto o en un formato no soportado."
            else:
                error_msg += "El archivo de imagen puede estar corrupto o en un formato no soportado."
            raise ValueError(error_msg)
        
        # Crear histograma del query
        query_histogram = self.codebook_builder.create_bow_histogram(query_features)
        
        # Realizar búsqueda según el método
        if method.lower() == 'sequential':
            results = self.knn_sequential.search(query_histogram, k)
        elif method.lower() == 'inverted':
            results = self.knn_inverted.search(query_histogram, k)
        else:
            raise ValueError("method debe ser 'sequential' o 'inverted'")
        
        return results
    
    def search_with_histogram(self, query_histogram: Any, k: int = 10,
                            method: str = 'inverted') -> List[Tuple[str, float]]:
        """
        Busca usando un histograma ya calculado
        
        Args:
            query_histogram: histograma de consulta
            k: número de resultados más similares
            method: 'sequential' o 'inverted'
            
        Returns:
            lista de (file_path, similarity_score)
        """
        if not self.is_built:
            raise ValueError("Los índices deben construirse antes de realizar búsquedas")
        
        if method.lower() == 'sequential':
            return self.knn_sequential.search(query_histogram, k)
        elif method.lower() == 'inverted':
            return self.knn_inverted.search(query_histogram, k)
        else:
            raise ValueError("method debe ser 'sequential' o 'inverted'")
    
    def benchmark_search_methods(self, query_path: str, k: int = 10) -> Dict[str, Any]:
        """
        Compara el rendimiento de los métodos de búsqueda
        
        Args:
            query_path: ruta del archivo de consulta
            k: número de resultados más similares
            
        Returns:
            diccionario con estadísticas de rendimiento
        """
        if not self.is_built:
            raise ValueError("Los índices deben construirse antes de realizar benchmarks")
        
        # Extraer características y crear histograma del query
        query_features = self.feature_extractor.extract_features(query_path)
        if query_features is None:
            raise ValueError(f"No se pudieron extraer características de: {query_path}")
            
        query_histogram = self.codebook_builder.create_bow_histogram(query_features)
        
        results = {}
        
        # Benchmark KNN secuencial
        start_time = time.time()
        seq_results = self.knn_sequential.search(query_histogram, k)
        seq_time = time.time() - start_time
        
        # Benchmark KNN con índice invertido
        start_time = time.time()
        inv_results = self.knn_inverted.search(query_histogram, k)
        inv_time = time.time() - start_time
        
        results = {
            'sequential': {
                'time': seq_time,
                'results': seq_results,
                'stats': self.knn_sequential.get_statistics()
            },
            'inverted': {
                'time': inv_time,
                'results': inv_results,
                'stats': self.knn_inverted.get_statistics()
            },
            'speedup': seq_time / inv_time if inv_time > 0 else float('inf')
        }
        
        return results
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """Obtiene estadísticas completas del sistema"""
        stats = {
            'media_type': self.media_type,
            'feature_method': self.feature_method,
            'n_clusters': self.n_clusters,
            'features_extracted': len(self.features_data),
            'histograms_created': len(self.histograms_data),
            'is_built': self.is_built
        }
        
        if self.codebook_builder.is_fitted:
            stats['codebook'] = self.codebook_builder.get_word_statistics(self.features_data)
        
        if self.is_built:
            stats['sequential_search'] = self.knn_sequential.get_statistics()
            stats['inverted_search'] = self.knn_inverted.get_statistics()
        
        return stats
    
    def save_complete_system(self, base_path: str = "embeddings/multimedia_system"):
        """Guarda todo el sistema construido"""
        os.makedirs(base_path, exist_ok=True)
        
        # Guardar características
        if self.features_data:
            self.feature_extractor.save_features(
                self.features_data, 
                os.path.join(base_path, "features.pkl")
            )
        
        # Guardar codebook
        if self.codebook_builder.is_fitted:
            self.codebook_builder.save_codebook(
                os.path.join(base_path, "codebook.pkl")
            )
        
        # Guardar histogramas
        if self.histograms_data:
            import pickle
            with open(os.path.join(base_path, "histograms.pkl"), 'wb') as f:
                pickle.dump(self.histograms_data, f)
        
        # Guardar índice invertido
        if self.is_built:
            self.knn_inverted.save_index(
                os.path.join(base_path, "inverted_index.pkl")
            )
        
        print(f"Sistema completo guardado en: {base_path}")
    
    def load_complete_system(self, base_path: str = "embeddings/multimedia_system"):
        """Carga un sistema previamente guardado"""
        # Cargar características
        features_path = os.path.join(base_path, "features.pkl")
        if os.path.exists(features_path):
            self.features_data = self.feature_extractor.load_features(features_path)
        
        # Cargar codebook
        codebook_path = os.path.join(base_path, "codebook.pkl")
        if os.path.exists(codebook_path):
            self.codebook_builder.load_codebook(codebook_path)
        
        # Cargar histogramas
        histograms_path = os.path.join(base_path, "histograms.pkl")
        if os.path.exists(histograms_path):
            import pickle
            with open(histograms_path, 'rb') as f:
                self.histograms_data = pickle.load(f)
        
        # Cargar índice invertido
        index_path = os.path.join(base_path, "inverted_index.pkl")
        if os.path.exists(index_path):
            self.knn_inverted.load_index(index_path)
            # Reconstruir índice secuencial
            if self.histograms_data:
                self.knn_sequential.build_database(self.histograms_data)
            self.is_built = True
        
        print(f"Sistema cargado desde: {base_path}")