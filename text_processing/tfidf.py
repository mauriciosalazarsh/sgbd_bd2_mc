import math
from typing import Dict, List, Tuple, Set
from collections import Counter, defaultdict
import pickle
import os

class TFIDFCalculator:
    """
    Clase para calcular pesos TF-IDF y similitud de coseno
    según los requerimientos del proyecto
    """
    
    def __init__(self):
        self.document_count = 0
        self.document_frequencies = defaultdict(int)  # DF: documentos que contienen cada término
        self.document_norms = {}  # Norma precalculada de cada documento
        self.vocabulary = set()
    
    def calculate_tf(self, term_count: int, total_terms: int, method: str = 'log') -> float:
        """
        Calcula Term Frequency
        
        Args:
            term_count: Frecuencia del término en el documento
            total_terms: Total de términos en el documento
            method: Método de cálculo ('raw', 'log', 'normalized')
            
        Returns:
            Valor TF calculado
        """
        if term_count == 0:
            return 0.0
        
        if method == 'raw':
            return term_count
        elif method == 'log':
            return 1 + math.log10(term_count)
        elif method == 'normalized':
            return term_count / total_terms
        else:
            return 1 + math.log10(term_count)  # Default
    
    def calculate_idf(self, term: str, smooth: bool = True) -> float:
        """
        Calcula Inverse Document Frequency
        
        Args:
            term: Término para calcular IDF
            smooth: Si aplicar suavizado para evitar división por cero
            
        Returns:
            Valor IDF calculado
        """
        df = self.document_frequencies.get(term, 0)
        
        if df == 0:
            return 0.0
        
        if smooth:
            return math.log10(self.document_count / df)
        else:
            return math.log10(self.document_count / df) if df > 0 else 0.0
    
    def calculate_tfidf(self, term: str, term_count: int, total_terms: int) -> float:
        """
        Calcula peso TF-IDF para un término en un documento
        
        Args:
            term: Término
            term_count: Frecuencia del término en el documento
            total_terms: Total de términos en el documento
            
        Returns:
            Peso TF-IDF
        """
        tf = self.calculate_tf(term_count, total_terms)
        idf = self.calculate_idf(term)
        return tf * idf
    
    def build_vocabulary_and_df(self, documents: List[List[str]]):
        """
        Construye el vocabulario y calcula document frequencies
        
        Args:
            documents: Lista de documentos tokenizados
        """
        self.document_count = len(documents)
        self.document_frequencies.clear()
        self.vocabulary.clear()
        
        for doc_tokens in documents:
            # Términos únicos en este documento
            unique_terms = set(doc_tokens)
            self.vocabulary.update(unique_terms)
            
            # Incrementar DF para cada término único en el documento
            for term in unique_terms:
                self.document_frequencies[term] += 1
    
    def calculate_document_tfidf_vector(self, doc_tokens: List[str], doc_id: int) -> Dict[str, float]:
        """
        Calcula el vector TF-IDF para un documento
        
        Args:
            doc_tokens: Tokens del documento
            doc_id: ID del documento
            
        Returns:
            Diccionario término -> peso TF-IDF
        """
        if not doc_tokens:
            return {}
        
        # Contar frecuencias de términos
        term_counts = Counter(doc_tokens)
        total_terms = len(doc_tokens)
        
        # Calcular TF-IDF para cada término
        tfidf_vector = {}
        for term, count in term_counts.items():
            tfidf_weight = self.calculate_tfidf(term, count, total_terms)
            if tfidf_weight > 0:  # Solo guardar pesos no cero
                tfidf_vector[term] = tfidf_weight
        
        return tfidf_vector
    
    def calculate_document_norm(self, tfidf_vector: Dict[str, float]) -> float:
        """
        Calcula la norma euclidiana del vector TF-IDF del documento
        
        Args:
            tfidf_vector: Vector TF-IDF del documento
            
        Returns:
            Norma del documento
        """
        if not tfidf_vector:
            return 0.0
        
        sum_of_squares = sum(weight ** 2 for weight in tfidf_vector.values())
        return math.sqrt(sum_of_squares)
    
    def precompute_document_norms(self, documents: List[List[str]]):
        """
        Precalcula las normas de todos los documentos para eficiencia
        
        Args:
            documents: Lista de documentos tokenizados
        """
        self.document_norms.clear()
        
        for doc_id, doc_tokens in enumerate(documents):
            tfidf_vector = self.calculate_document_tfidf_vector(doc_tokens, doc_id)
            norm = self.calculate_document_norm(tfidf_vector)
            self.document_norms[doc_id] = norm
    
    def calculate_query_tfidf_vector(self, query_tokens: List[str]) -> Dict[str, float]:
        """
        Calcula el vector TF-IDF para una consulta
        
        Args:
            query_tokens: Tokens de la consulta
            
        Returns:
            Vector TF-IDF de la consulta
        """
        if not query_tokens:
            return {}
        
        term_counts = Counter(query_tokens)
        total_terms = len(query_tokens)
        
        query_vector = {}
        for term, count in term_counts.items():
            if term in self.vocabulary:  # Solo términos conocidos
                tfidf_weight = self.calculate_tfidf(term, count, total_terms)
                if tfidf_weight > 0:
                    query_vector[term] = tfidf_weight
        
        return query_vector
    
    def cosine_similarity(self, query_vector: Dict[str, float], 
                         doc_vector: Dict[str, float], 
                         doc_norm: float) -> float:
        """
        Calcula similitud de coseno entre consulta y documento
        
        Args:
            query_vector: Vector TF-IDF de la consulta
            doc_vector: Vector TF-IDF del documento
            doc_norm: Norma precalculada del documento
            
        Returns:
            Similitud de coseno [0, 1]
        """
        if not query_vector or not doc_vector or doc_norm == 0:
            return 0.0
        
        # Calcular producto punto
        dot_product = 0.0
        for term, query_weight in query_vector.items():
            if term in doc_vector:
                dot_product += query_weight * doc_vector[term]
        
        if dot_product == 0:
            return 0.0
        
        # Calcular norma de la consulta
        query_norm = math.sqrt(sum(weight ** 2 for weight in query_vector.values()))
        
        if query_norm == 0:
            return 0.0
        
        # Similitud de coseno
        return dot_product / (query_norm * doc_norm)
    
    def get_stats(self) -> Dict:
        """
        Obtiene estadísticas del calculador
        
        Returns:
            Diccionario con estadísticas
        """
        return {
            'vocabulary_size': len(self.vocabulary),
            'document_count': self.document_count,
            'avg_df': sum(self.document_frequencies.values()) / len(self.document_frequencies) if self.document_frequencies else 0,
            'terms_with_df_1': sum(1 for df in self.document_frequencies.values() if df == 1)
        }
    
    def save_model(self, filepath: str):
        """
        Guarda el modelo TF-IDF
        
        Args:
            filepath: Ruta donde guardar el modelo
        """
        model_data = {
            'document_count': self.document_count,
            'document_frequencies': dict(self.document_frequencies),
            'document_norms': self.document_norms,
            'vocabulary': self.vocabulary
        }
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
    
    def load_model(self, filepath: str):
        """
        Carga el modelo TF-IDF
        
        Args:
            filepath: Ruta del modelo a cargar
        """
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.document_count = model_data['document_count']
        self.document_frequencies = defaultdict(int, model_data['document_frequencies'])
        self.document_norms = model_data['document_norms']
        self.vocabulary = model_data['vocabulary']


class BatchTFIDFProcessor:
    """
    Procesador por lotes para manejar grandes colecciones de documentos
    """
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.tfidf_calc = TFIDFCalculator()
    
    def process_documents_in_batches(self, documents: List[List[str]]) -> List[Dict[str, float]]:
        """
        Procesa documentos en lotes para mayor eficiencia
        
        Args:
            documents: Lista de documentos tokenizados
            
        Returns:
            Lista de vectores TF-IDF
        """
        # Primero construir vocabulario y DF con todos los documentos
        print("Construyendo vocabulario y calculando document frequencies...")
        self.tfidf_calc.build_vocabulary_and_df(documents)
        
        # Procesar documentos en lotes
        all_vectors = []
        total_docs = len(documents)
        
        for i in range(0, total_docs, self.batch_size):
            batch_end = min(i + self.batch_size, total_docs)
            batch_docs = documents[i:batch_end]
            
            print(f"Procesando lote {i//self.batch_size + 1}: documentos {i+1}-{batch_end}")
            
            batch_vectors = []
            for doc_id, doc_tokens in enumerate(batch_docs, start=i):
                vector = self.tfidf_calc.calculate_document_tfidf_vector(doc_tokens, doc_id)
                batch_vectors.append(vector)
            
            all_vectors.extend(batch_vectors)
        
        # Precalcular normas
        print("Calculando normas de documentos...")
        self.tfidf_calc.precompute_document_norms(documents)
        
        return all_vectors