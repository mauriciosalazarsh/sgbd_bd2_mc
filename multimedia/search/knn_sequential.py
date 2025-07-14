# multimedia/search/knn_sequential.py - Versión corregida
import numpy as np
import heapq
import time
from typing import List, Tuple, Optional

# Importación segura de sklearn
try:
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    cosine_similarity = None

class KNNSequential:
    def __init__(self, use_tfidf=True):
        """
        Implementación de KNN secuencial para búsqueda multimedia
        
        Args:
            use_tfidf: usar ponderación TF-IDF en los histogramas
        """
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn no está instalado. Ejecuta: pip install scikit-learn")
            
        self.use_tfidf = use_tfidf
        self.database: List[Tuple[str, np.ndarray]] = []
        self.tfidf_transformer: Optional[MultimediaTFIDF] = None
        self.weighted_histograms: Optional[np.ndarray] = None
        
    def build_database(self, histograms_data: List[Tuple[str, np.ndarray]]):
        """
        Construye la base de datos de histogramas
        
        Args:
            histograms_data: lista de (file_path, histogram)
        """
        if not histograms_data:
            raise ValueError("No hay datos de histogramas para construir la base de datos")
            
        self.database = histograms_data
        
        if self.use_tfidf:
            # Aplicar TF-IDF a los histogramas
            self.tfidf_transformer = MultimediaTFIDF()
            histograms_matrix = np.vstack([hist for _, hist in histograms_data])
            self.weighted_histograms = self.tfidf_transformer.fit_transform(histograms_matrix)
        else:
            # Usar histogramas originales
            self.weighted_histograms = np.vstack([hist for _, hist in histograms_data])
        
        print(f"Base de datos construida con {len(self.database)} objetos")
    
    def search(self, query_histogram: np.ndarray, k: int = 10) -> List[Tuple[str, float]]:
        """
        Búsqueda KNN secuencial
        
        Args:
            query_histogram: histograma de consulta
            k: número de resultados más similares
            
        Returns:
            lista de (file_path, similarity_score) ordenada por similitud
        """
        if len(self.database) == 0:
            return []
        
        if self.weighted_histograms is None:
            raise ValueError("Base de datos no inicializada")
        
        if cosine_similarity is None:
            raise RuntimeError("scikit-learn no está disponible")
            
        start_time = time.time()
        
        # Aplicar TF-IDF al query si es necesario
        if self.use_tfidf and self.tfidf_transformer is not None:
            query_weighted = self.tfidf_transformer.transform(query_histogram.reshape(1, -1))
        else:
            query_weighted = query_histogram.reshape(1, -1)
        
        # Calcular similitudes usando heap para mantener top-k eficientemente
        similarities_heap: List[Tuple[float, str]] = []
        
        for i, (file_path, _) in enumerate(self.database):
            # Calcular similitud de coseno
            db_histogram = self.weighted_histograms[i].reshape(1, -1)
            similarity = cosine_similarity(query_weighted, db_histogram)[0][0]
            
            # Mantener heap de tamaño k con los mejores resultados
            if len(similarities_heap) < k:
                heapq.heappush(similarities_heap, (similarity, file_path))
            elif similarity > similarities_heap[0][0]:
                heapq.heapreplace(similarities_heap, (similarity, file_path))
        
        # Convertir heap a lista ordenada (mayor a menor similitud)
        results = []
        while similarities_heap:
            similarity, file_path = heapq.heappop(similarities_heap)
            results.append((file_path, similarity))
        
        results.reverse()  # Ordenar de mayor a menor similitud
        
        search_time = time.time() - start_time
        print(f"Búsqueda completada en {search_time:.4f} segundos")
        
        return results
    
    def search_with_threshold(self, query_histogram: np.ndarray, 
                            threshold: float = 0.1) -> List[Tuple[str, float]]:
        """
        Búsqueda con umbral de similitud mínima
        
        Args:
            query_histogram: histograma de consulta
            threshold: similitud mínima requerida
            
        Returns:
            lista de (file_path, similarity_score) que superan el umbral
        """
        if len(self.database) == 0:
            return []
        
        if self.weighted_histograms is None:
            raise ValueError("Base de datos no inicializada")
        
        if cosine_similarity is None:
            raise RuntimeError("scikit-learn no está disponible")
        
        # Aplicar TF-IDF al query si es necesario
        if self.use_tfidf and self.tfidf_transformer is not None:
            query_weighted = self.tfidf_transformer.transform(query_histogram.reshape(1, -1))
        else:
            query_weighted = query_histogram.reshape(1, -1)
        
        results = []
        
        for i, (file_path, _) in enumerate(self.database):
            # Calcular similitud de coseno
            db_histogram = self.weighted_histograms[i].reshape(1, -1)
            similarity = cosine_similarity(query_weighted, db_histogram)[0][0]
            
            # Agregar si supera el umbral
            if similarity >= threshold:
                results.append((file_path, similarity))
        
        # Ordenar por similitud descendente
        results.sort(key=lambda x: x[1], reverse=True)
        
        return results
    
    def get_statistics(self) -> dict:
        """Obtiene estadísticas de la base de datos"""
        if len(self.database) == 0:
            return {}
        
        if self.weighted_histograms is None:
            return {'error': 'Base de datos no inicializada'}
        
        histograms = self.weighted_histograms
        
        return {
            'num_objects': len(self.database),
            'histogram_dimension': histograms.shape[1],
            'mean_histogram_norm': float(np.mean(np.linalg.norm(histograms, axis=1))),
            'sparsity': float(np.mean(np.sum(histograms == 0, axis=1) / histograms.shape[1])),
            'use_tfidf': self.use_tfidf
        }


class MultimediaTFIDF:
    """Implementación de TF-IDF para histogramas multimedia"""
    
    def __init__(self):
        self.idf_weights: Optional[np.ndarray] = None
        self.is_fitted = False
    
    def fit(self, histograms: np.ndarray) -> 'MultimediaTFIDF':
        """
        Calcula los pesos IDF para el vocabulario
        
        Args:
            histograms: matriz de histogramas (n_documents, n_words)
        """
        if histograms.size == 0:
            raise ValueError("Matriz de histogramas vacía")
            
        n_documents = histograms.shape[0]
        
        # Calcular document frequency para cada word
        df = np.sum(histograms > 0, axis=0)
        
        # Calcular IDF (suavizado para evitar división por cero)
        self.idf_weights = np.log(n_documents / (df + 1)) + 1
        self.is_fitted = True
        
        return self
    
    def transform(self, histograms: np.ndarray) -> np.ndarray:
        """
        Aplica TF-IDF a los histogramas
        
        Args:
            histograms: matriz de histogramas
            
        Returns:
            matriz de histogramas ponderados con TF-IDF
        """
        if not self.is_fitted or self.idf_weights is None:
            raise ValueError("El transformador TF-IDF no ha sido entrenado")
        
        if histograms.ndim == 1:
            histograms = histograms.reshape(1, -1)
        
        # TF-IDF = TF * IDF
        tfidf_histograms = histograms * self.idf_weights
        
        # Normalizar cada histograma
        norms = np.linalg.norm(tfidf_histograms, axis=1, keepdims=True)
        norms[norms == 0] = 1  # Evitar división por cero
        tfidf_histograms = tfidf_histograms / norms
        
        return tfidf_histograms
    
    def fit_transform(self, histograms: np.ndarray) -> np.ndarray:
        """Entrena y transforma en un solo paso"""
        return self.fit(histograms).transform(histograms)