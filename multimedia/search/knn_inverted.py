# multimedia/search/knn_inverted.py - Versión corregida
import os
import numpy as np
import heapq
import time
from collections import defaultdict
from typing import List, Tuple, Dict, Optional
from .knn_sequential import MultimediaTFIDF

# Importación segura de sklearn
try:
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    cosine_similarity = None

class KNNInvertedIndex:
    def __init__(self, use_tfidf=True):
        """
        Implementación de KNN con índice invertido para búsqueda multimedia
        
        Args:
            use_tfidf: usar ponderación TF-IDF
        """
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn no está instalado. Ejecuta: pip install scikit-learn")
            
        self.use_tfidf = use_tfidf
        self.inverted_index: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
        self.documents: Dict[int, Tuple[str, np.ndarray]] = {}
        self.document_norms: Dict[int, float] = {}
        self.tfidf_transformer: Optional[MultimediaTFIDF] = None
        self.vocab_size = 0
        
    def build_index(self, histograms_data: List[Tuple[str, np.ndarray]]):
        """
        Construye el índice invertido a partir de histogramas
        
        Args:
            histograms_data: lista de (file_path, histogram)
        """
        if not histograms_data:
            raise ValueError("No hay datos de histogramas para construir el índice")
            
        print("\n🔍 Construyendo índice invertido KNN...")
        print("=" * 50)
        start_time = time.time()
        
        # Limpiar estructuras previas
        self.inverted_index.clear()
        self.documents.clear()
        self.document_norms.clear()
        
        # Obtener dimensión del vocabulario
        self.vocab_size = histograms_data[0][1].shape[0]
        print(f"📊 Dimensión del vocabulario: {self.vocab_size}")
        
        # Construir matriz de histogramas para TF-IDF
        if self.use_tfidf:
            histograms_matrix = np.vstack([hist for _, hist in histograms_data])
            self.tfidf_transformer = MultimediaTFIDF()
            weighted_histograms = self.tfidf_transformer.fit_transform(histograms_matrix)
        else:
            weighted_histograms = np.vstack([hist for _, hist in histograms_data])
        
        # Construir índice invertido
        total_docs = len(histograms_data)
        print(f"📝 Indexando {total_docs} documentos...")
        
        for doc_id, (file_path, histogram) in enumerate(histograms_data):
            if doc_id % 100 == 0 or doc_id == total_docs - 1:
                progress = (doc_id + 1) / total_docs * 100
                print(f"\rProgreso: {progress:.1f}% ({doc_id + 1}/{total_docs})", end='', flush=True)
            # Usar histograma ponderado con TF-IDF si está habilitado
            if self.use_tfidf:
                weighted_histogram = weighted_histograms[doc_id]
            else:
                weighted_histogram = histogram
            
            # Almacenar documento
            self.documents[doc_id] = (file_path, weighted_histogram)
            
            # Calcular y almacenar norma del documento
            self.document_norms[doc_id] = float(np.linalg.norm(weighted_histogram))
            
            # Agregar términos no cero al índice invertido
            for word_id, weight in enumerate(weighted_histogram):
                if weight > 0:
                    self.inverted_index[word_id].append((doc_id, float(weight)))
        
        build_time = time.time() - start_time
        print(f"\n✅ Índice construido en {build_time:.2f} segundos")
        print(f"📊 Documentos indexados: {len(self.documents)}")
        print(f"📊 Términos en vocabulario: {len(self.inverted_index)}")
        
    def search(self, query_histogram: np.ndarray, k: int = 10) -> List[Tuple[str, float]]:
        """
        Búsqueda KNN usando índice invertido
        
        Args:
            query_histogram: histograma de consulta
            k: número de resultados más similares
            
        Returns:
            lista de (file_path, similarity_score) ordenada por similitud
        """
        if len(self.documents) == 0:
            return []
        
        start_time = time.time()
        
        # Aplicar TF-IDF al query si es necesario
        if self.use_tfidf and self.tfidf_transformer is not None:
            query_weighted = self.tfidf_transformer.transform(query_histogram.reshape(1, -1))[0]
        else:
            query_weighted = query_histogram
        
        # Encontrar documentos candidatos usando el índice invertido
        candidate_scores: Dict[int, float] = defaultdict(float)
        query_norm = float(np.linalg.norm(query_weighted))
        
        # Solo procesar términos no cero en la consulta
        for word_id, query_weight in enumerate(query_weighted):
            if query_weight > 0 and word_id in self.inverted_index:
                # Para cada documento que contiene este término
                for doc_id, doc_weight in self.inverted_index[word_id]:
                    candidate_scores[doc_id] += float(query_weight * doc_weight)
        
        # Calcular similitudes de coseno para candidatos
        similarities_heap: List[Tuple[float, str]] = []
        
        for doc_id, dot_product in candidate_scores.items():
            # Similitud de coseno = dot_product / (norm_query * norm_doc)
            doc_norm = self.document_norms[doc_id]
            if query_norm > 0 and doc_norm > 0:
                similarity = dot_product / (query_norm * doc_norm)
            else:
                similarity = 0.0
            
            file_path = self.documents[doc_id][0]
            
            # Mantener heap de tamaño k con los mejores resultados
            if len(similarities_heap) < k:
                heapq.heappush(similarities_heap, (similarity, file_path))
            elif similarity > similarities_heap[0][0]:
                heapq.heapreplace(similarities_heap, (similarity, file_path))
        
        # Convertir heap a lista ordenada
        results = []
        while similarities_heap:
            similarity, file_path = heapq.heappop(similarities_heap)
            results.append((file_path, similarity))
        
        results.reverse()  # Ordenar de mayor a menor similitud
        
        search_time = time.time() - start_time
        print(f"Búsqueda con índice invertido completada en {search_time:.4f} segundos")
        print(f"Documentos candidatos evaluados: {len(candidate_scores)}")
        
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
        if len(self.documents) == 0:
            return []
        
        # Aplicar TF-IDF al query si es necesario
        if self.use_tfidf and self.tfidf_transformer is not None:
            query_weighted = self.tfidf_transformer.transform(query_histogram.reshape(1, -1))[0]
        else:
            query_weighted = query_histogram
        
        # Encontrar documentos candidatos
        candidate_scores: Dict[int, float] = defaultdict(float)
        query_norm = float(np.linalg.norm(query_weighted))
        
        for word_id, query_weight in enumerate(query_weighted):
            if query_weight > 0 and word_id in self.inverted_index:
                for doc_id, doc_weight in self.inverted_index[word_id]:
                    candidate_scores[doc_id] += float(query_weight * doc_weight)
        
        # Filtrar por umbral
        results = []
        for doc_id, dot_product in candidate_scores.items():
            doc_norm = self.document_norms[doc_id]
            if query_norm > 0 and doc_norm > 0:
                similarity = dot_product / (query_norm * doc_norm)
                if similarity >= threshold:
                    file_path = self.documents[doc_id][0]
                    results.append((file_path, similarity))
        
        # Ordenar por similitud descendente
        results.sort(key=lambda x: x[1], reverse=True)
        
        return results
    
    def get_statistics(self) -> dict:
        """Obtiene estadísticas del índice"""
        if len(self.documents) == 0:
            return {}
        
        # Estadísticas del índice invertido
        total_postings = sum(len(postings) for postings in self.inverted_index.values())
        avg_postings_per_term = total_postings / len(self.inverted_index) if self.inverted_index else 0
        
        # Estadísticas de documentos
        doc_lengths = []
        for doc_id, (_, histogram) in self.documents.items():
            doc_length = int(np.sum(histogram > 0))
            doc_lengths.append(doc_length)
        
        avg_doc_length = float(np.mean(doc_lengths)) if doc_lengths else 0
        
        return {
            'num_documents': len(self.documents),
            'vocab_size': self.vocab_size,
            'terms_in_index': len(self.inverted_index),
            'total_postings': total_postings,
            'avg_postings_per_term': avg_postings_per_term,
            'avg_document_length': avg_doc_length,
            'use_tfidf': self.use_tfidf,
            'compression_ratio': len(self.inverted_index) / self.vocab_size if self.vocab_size > 0 else 0
        }
    
    def save_index(self, save_path: str):
        """Guarda el índice invertido en disco"""
        import pickle
        
        index_data = {
            'inverted_index': dict(self.inverted_index),
            'documents': self.documents,
            'document_norms': self.document_norms,
            'tfidf_transformer': self.tfidf_transformer,
            'vocab_size': self.vocab_size,
            'use_tfidf': self.use_tfidf
        }
        
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, 'wb') as f:
                pickle.dump(index_data, f)
            print(f"Índice guardado en: {save_path}")
        except Exception as e:
            print(f"Error guardando índice: {e}")
    
    def load_index(self, load_path: str):
        """Carga un índice invertido desde disco"""
        import pickle
        
        try:
            with open(load_path, 'rb') as f:
                index_data = pickle.load(f)
            
            self.inverted_index = defaultdict(list, index_data['inverted_index'])
            self.documents = index_data['documents']
            self.document_norms = index_data['document_norms']
            self.tfidf_transformer = index_data['tfidf_transformer']
            self.vocab_size = index_data['vocab_size']
            self.use_tfidf = index_data['use_tfidf']
            
            print(f"Índice cargado desde: {load_path}")
            print(f"Documentos: {len(self.documents)}, Términos: {len(self.inverted_index)}")
        except Exception as e:
            print(f"Error cargando índice: {e}")