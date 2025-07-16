"""
Índice Invertido para búsqueda de texto con TF-IDF y similitud de coseno.
Versión corregida integrada con el sistema de base de datos multimedia existente.
"""

import os
import pickle
import heapq
import time
import math
import re
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict, Counter

# Imports con manejo de errores para compatibilidad
try:
    from text_processing.preprocessor import TextPreprocessor
    from text_processing.tfidf import TFIDFCalculator, BatchTFIDFProcessor
    TEXT_PROCESSING_AVAILABLE = True
except ImportError:
    print(" Módulos de text_processing no encontrados. Usando versión simplificada.")
    TEXT_PROCESSING_AVAILABLE = False

try:
    from indices.spimi import SPIMIIndexBuilder
    SPIMI_AVAILABLE = True
except ImportError:
    print(" SPIMI no encontrado, usando construcción en memoria.")
    SPIMI_AVAILABLE = False

# Versión simplificada de procesamiento de texto
class SimpleTextProcessor:
    """Procesador de texto básico sin dependencias externas"""
    
    def __init__(self, language='spanish'):
        self.language = language
        self.setup_stopwords()
    
    def setup_stopwords(self):
        """Configurar stopwords básicas sin NLTK"""
        if self.language == 'spanish':
            self.stopwords = {
                'el', 'la', 'de', 'que', 'y', 'a', 'en', 'un', 'es', 'se', 'no', 'te', 
                'lo', 'le', 'da', 'su', 'por', 'son', 'con', 'para', 'al', 'del', 'los', 
                'las', 'una', 'como', 'todo', 'pero', 'más', 'me', 'ya', 'muy', 'fue'
            }
        else:
            self.stopwords = {
                'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
                'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'have'
            }
    
    def preprocess(self, text: str) -> List[str]:
        """Procesamiento básico de texto"""
        if not text or not isinstance(text, str):
            return []
        
        # Limpiar y normalizar
        text = text.lower()
        
        # Quitar acentos básicos
        replacements = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ñ': 'n'}
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Extraer palabras
        words = re.findall(r'\b[a-z]+\b', text)
        
        # Filtrar stopwords y palabras cortas
        words = [w for w in words if len(w) >= 3 and w not in self.stopwords]
        
        return words
    
    def concatenate_fields(self, record: dict, text_fields: List[str]) -> str:
        """Concatena campos textuales de un registro"""
        texts = []
        for field in text_fields:
            if field in record and record[field]:
                texts.append(str(record[field]))
        return ' '.join(texts)

# Calculador TF-IDF simplificado
class SimpleTFIDFCalculator:
    """Calculador TF-IDF básico sin dependencias externas"""
    
    def __init__(self):
        self.document_count = 0
        self.document_frequencies = defaultdict(int)
        self.document_norms = {}
        self.vocabulary = set()
    
    def build_vocabulary_and_df(self, documents: List[List[str]]):
        """Construye vocabulario y document frequencies"""
        self.document_count = len(documents)
        self.document_frequencies.clear()
        self.vocabulary.clear()
        
        for doc_tokens in documents:
            unique_terms = set(doc_tokens)
            self.vocabulary.update(unique_terms)
            
            for term in unique_terms:
                self.document_frequencies[term] += 1
    
    def calculate_document_tfidf_vector(self, doc_tokens: List[str], doc_id: int) -> Dict[str, float]:
        """Calcula vector TF-IDF para un documento"""
        if not doc_tokens:
            return {}
        
        term_counts = Counter(doc_tokens)
        total_terms = len(doc_tokens)
        
        tfidf_vector = {}
        for term, count in term_counts.items():
            # TF logarítmico
            tf = 1 + math.log10(count)
            # IDF
            df = self.document_frequencies.get(term, 0)
            idf = math.log10(self.document_count / df) if df > 0 else 0
            # TF-IDF
            tfidf_weight = tf * idf
            
            if tfidf_weight > 0:
                tfidf_vector[term] = tfidf_weight
        
        return tfidf_vector
    
    def calculate_query_tfidf_vector(self, query_tokens: List[str]) -> Dict[str, float]:
        """Calcula vector TF-IDF para una consulta"""
        if not query_tokens:
            return {}
        
        term_counts = Counter(query_tokens)
        total_terms = len(query_tokens)
        
        query_vector = {}
        for term, count in term_counts.items():
            if term in self.vocabulary:
                tf = 1 + math.log10(count)
                df = self.document_frequencies.get(term, 0)
                idf = math.log10(self.document_count / df) if df > 0 else 0
                tfidf_weight = tf * idf
                
                if tfidf_weight > 0:
                    query_vector[term] = tfidf_weight
        
        return query_vector
    
    def cosine_similarity(self, query_vector: Dict[str, float], 
                         doc_vector: Dict[str, float], 
                         doc_norm: float) -> float:
        """Calcula similitud de coseno"""
        if not query_vector or not doc_vector or doc_norm == 0:
            return 0.0
        
        # Producto punto
        dot_product = 0.0
        for term, query_weight in query_vector.items():
            if term in doc_vector:
                dot_product += query_weight * doc_vector[term]
        
        if dot_product == 0:
            return 0.0
        
        # Norma de la consulta
        query_norm = math.sqrt(sum(weight ** 2 for weight in query_vector.values()))
        
        if query_norm == 0:
            return 0.0
        
        return dot_product / (query_norm * doc_norm)


class InvertedIndex:
    """
    Índice Invertido con TF-IDF para búsqueda de texto libre
    Compatible con el sistema de base de datos multimedia existente
    """
    
    def __init__(self, index_name: str, text_fields: List[str], language: str = 'spanish'):
        """
        Inicializa el índice invertido
        
        Args:
            index_name: Nombre del índice
            text_fields: Campos de texto a indexar
            language: Idioma para procesamiento ('spanish' o 'english')
        """
        self.index_name = index_name
        self.text_fields = text_fields
        self.language = language
        
        # Componentes de procesamiento - con fallback
        if TEXT_PROCESSING_AVAILABLE:
            try:
                self.preprocessor = TextPreprocessor(language)
                self.tfidf_calculator = TFIDFCalculator()
                print(" Usando componentes avanzados con NLTK")
            except:
                self.preprocessor = SimpleTextProcessor(language)
                self.tfidf_calculator = SimpleTFIDFCalculator()
                print(" Fallback a componentes simplificados")
        else:
            self.preprocessor = SimpleTextProcessor(language)
            self.tfidf_calculator = SimpleTFIDFCalculator()
            print(" Usando componentes simplificados")
        
        # Estructura del índice
        self.inverted_index = {}  # term -> [(doc_id, tfidf_weight)]
        self.document_metadata = {}  # doc_id -> metadata original
        self.total_documents = 0
        
        # Archivos de persistencia - compatibles con estructura existente
        self.index_file = f"embeddings/{index_name}_inverted_index.pkl"
        self.metadata_file = f"embeddings/{index_name}_metadata.pkl"
        self.stats_file = f"embeddings/{index_name}_stats.pkl"
        
        # Asegurar que existe el directorio embeddings
        os.makedirs('embeddings', exist_ok=True)
        
        # Configuración
        self.use_spimi = SPIMI_AVAILABLE and len(text_fields) > 0
        self.spimi_memory_limit = 50  # MB - más conservador
    
    # Métodos requeridos por compatibilidad con el engine existente
    def scan_all(self) -> List[Dict]:
        """Retorna todos los documentos indexados"""
        return list(self.document_metadata.values())
    
    def insert(self, key: Any, values: List[str]) -> None:
        """No implementado - usar build_index_from_data()"""
        raise NotImplementedError("Use build_index_from_data() para construir el índice completo")
    
    def search(self, query: str, k: int = 10) -> List[Tuple[Dict, float]]:
        """
        Busca documentos similares a la consulta usando similitud de coseno
        
        Args:
            query: Consulta en lenguaje natural
            k: Número de resultados a retornar
            
        Returns:
            Lista de (documento, score) ordenados por relevancia
        """
        if not self._is_index_loaded():
            if not self._load_index():
                print(" No se pudo cargar el índice")
                return []
        
        start_time = time.time()
        print(f" Buscando: '{query}'")
        
        # Preprocesar consulta
        query_tokens = self.preprocessor.preprocess(query)
        if not query_tokens:
            print(" Consulta vacía después del procesamiento")
            return []
        
        print(f" Tokens de consulta: {query_tokens}")
        
        # Calcular vector TF-IDF de la consulta
        query_vector = self.tfidf_calculator.calculate_query_tfidf_vector(query_tokens)
        if not query_vector:
            print(" Vector de consulta vacío")
            return []
        
        # Obtener documentos candidatos
        candidate_docs = self._get_candidate_documents(query_vector)
        print(f" Documentos candidatos: {len(candidate_docs)}")
        
        # Calcular similitudes
        scores = self._calculate_similarities(query_vector, candidate_docs)
        
        # Retornar top-k resultados
        top_k_results = heapq.nlargest(k, scores, key=lambda x: x[1])
        
        search_time = time.time() - start_time
        print(f" Búsqueda completada en {search_time:.3f}s - {len(top_k_results)} resultados")
        
        return top_k_results
    
    def build_index_from_data(self, data: List[dict], progress_callback=None) -> bool:
        """
        Construye el índice invertido desde una lista de documentos
        
        Args:
            data: Lista de diccionarios con los documentos
            progress_callback: Función de callback para progreso
            
        Returns:
            True si la construcción fue exitosa
        """
        try:
            print(f" Construyendo índice invertido: {len(data)} documentos")
            print(f" Campos de texto: {self.text_fields}")
            
            # Paso 1: Preprocesar documentos
            processed_docs, doc_metadata = self._preprocess_documents(data, progress_callback)
            
            if not processed_docs:
                print(" No se procesaron documentos válidos")
                return False
            
            # Paso 2: Calcular TF-IDF
            tfidf_vectors = self._calculate_tfidf_vectors(processed_docs)
            
            # Paso 3: Construir índice invertido
            if SPIMI_AVAILABLE and self.use_spimi and len(data) > 500:
                print(" Usando algoritmo SPIMI para construcción")
                self._build_index_with_spimi(processed_docs, tfidf_vectors)
            else:
                print(" Construyendo índice en memoria")
                self._build_index_in_memory(processed_docs, tfidf_vectors)
            
            # Paso 4: Guardar metadatos y estadísticas
            self.document_metadata = doc_metadata
            self.total_documents = len(data)
            self._save_index()
            
            print(" Índice construido exitosamente!")
            return True
            
        except Exception as e:
            print(f" Error construyendo índice: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _preprocess_documents(self, data: List[dict], progress_callback=None) -> Tuple[List[Tuple[int, List[str]]], Dict[int, dict]]:
        """Preprocesa los documentos concatenando campos de texto"""
        processed_docs = []
        doc_metadata = {}
        
        print(" Preprocesando documentos...")
        
        for doc_id, record in enumerate(data):
            if progress_callback and doc_id % 100 == 0:
                progress_callback(f"Preprocesando documento {doc_id + 1}/{len(data)}")
            
            # Concatenar campos de texto
            text_content = self.preprocessor.concatenate_fields(record, self.text_fields)
            
            # Procesar texto
            tokens = self.preprocessor.preprocess(text_content)
            
            if tokens:  # Solo incluir documentos con contenido
                processed_docs.append((doc_id, tokens))
                doc_metadata[doc_id] = record.copy()
        
        print(f" Preprocesados {len(processed_docs)} documentos con contenido")
        return processed_docs, doc_metadata
    
    def _calculate_tfidf_vectors(self, processed_docs: List[Tuple[int, List[str]]]) -> List[Dict[str, float]]:
        """Calcula vectores TF-IDF para todos los documentos"""
        print(" Calculando vectores TF-IDF...")
        
        # Extraer solo los tokens para el cálculo
        documents_tokens = [tokens for _, tokens in processed_docs]
        
        # Construir vocabulario y DF
        self.tfidf_calculator.build_vocabulary_and_df(documents_tokens)
        
        # Calcular vectores TF-IDF
        tfidf_vectors = []
        for doc_id, tokens in enumerate(documents_tokens):
            vector = self.tfidf_calculator.calculate_document_tfidf_vector(tokens, doc_id)
            tfidf_vectors.append(vector)
        
        # Precalcular normas de documentos
        for doc_id, vector in enumerate(tfidf_vectors):
            if vector:
                norm = math.sqrt(sum(weight ** 2 for weight in vector.values()))
                self.tfidf_calculator.document_norms[doc_id] = norm
            else:
                self.tfidf_calculator.document_norms[doc_id] = 0.0
        
        print(f" Calculados {len(tfidf_vectors)} vectores TF-IDF")
        return tfidf_vectors
    
    def _build_index_with_spimi(self, processed_docs: List[Tuple[int, List[str]]], 
                               tfidf_vectors: List[Dict[str, float]]):
        """Construye el índice usando SPIMI para grandes colecciones"""
        if not SPIMI_AVAILABLE:
            print(" SPIMI no disponible, usando construcción en memoria")
            self._build_index_in_memory(processed_docs, tfidf_vectors)
            return
            
        try:
            # Crear directorio temporal
            temp_dir = f"temp_{self.index_name}"
            
            # Configurar SPIMI
            spimi_builder = SPIMIIndexBuilder(
                output_dir="indices",
                memory_limit_mb=self.spimi_memory_limit,
                temp_dir=temp_dir
            )
            
            # Construir índice
            index_file = spimi_builder.build_index(processed_docs, tfidf_vectors)
            
            # Cargar índice construido
            with open(index_file, 'rb') as f:
                index_data = pickle.load(f)
                self.inverted_index = index_data['index']
            
            # Limpiar archivo temporal
            if os.path.exists(index_file):
                os.remove(index_file)
        except Exception as e:
            print(f" Error con SPIMI: {e}")
            self._build_index_in_memory(processed_docs, tfidf_vectors)
    
    def _build_index_in_memory(self, processed_docs: List[Tuple[int, List[str]]], 
                              tfidf_vectors: List[Dict[str, float]]):
        """Construye el índice completamente en memoria"""
        self.inverted_index = defaultdict(list)
        
        for i, (doc_id, tokens) in enumerate(processed_docs):
            tfidf_vector = tfidf_vectors[i] if i < len(tfidf_vectors) else {}
            
            for term, weight in tfidf_vector.items():
                self.inverted_index[term].append((doc_id, weight))
        
        # Convertir a diccionario normal
        self.inverted_index = dict(self.inverted_index)
    
    def _get_candidate_documents(self, query_vector: Dict[str, float]) -> set:
        """Obtiene documentos candidatos que contienen términos de la consulta"""
        candidate_docs = set()
        
        for term in query_vector.keys():
            if term in self.inverted_index:
                postings = self.inverted_index[term]
                for doc_id, _ in postings:
                    candidate_docs.add(doc_id)
        
        return candidate_docs
    
    def _calculate_similarities(self, query_vector: Dict[str, float], 
                               candidate_docs: set) -> List[Tuple[Dict, float]]:
        """Calcula similitudes de coseno para documentos candidatos"""
        scores = []
        
        for doc_id in candidate_docs:
            # Construir vector del documento
            doc_vector = {}
            for term in query_vector.keys():
                if term in self.inverted_index:
                    # Buscar el peso para este documento
                    postings = self.inverted_index[term]
                    for d_id, weight in postings:
                        if d_id == doc_id:
                            doc_vector[term] = weight
                            break
            
            # Calcular similitud
            doc_norm = self.tfidf_calculator.document_norms.get(doc_id, 0)
            if doc_norm > 0:
                similarity = self.tfidf_calculator.cosine_similarity(
                    query_vector, doc_vector, doc_norm
                )
                
                if similarity > 0:
                    doc_metadata = self.document_metadata.get(doc_id, {})
                    scores.append((doc_metadata, similarity))
        
        return scores
    
    def _save_index(self):
        """Guarda el índice y metadatos al disco"""
        print(" Guardando índice al disco...")
        
        try:
            # Guardar índice principal
            with open(self.index_file, 'wb') as f:
                pickle.dump(self.inverted_index, f)
            
            # Guardar metadatos
            with open(self.metadata_file, 'wb') as f:
                pickle.dump({
                    'document_metadata': self.document_metadata,
                    'document_norms': self.tfidf_calculator.document_norms,
                    'total_documents': self.total_documents,
                    'text_fields': self.text_fields,
                    'language': self.language
                }, f)
            
            # Guardar estadísticas
            stats = self._calculate_index_stats()
            with open(self.stats_file, 'wb') as f:
                pickle.dump(stats, f)
            
            print(f" Índice guardado: {self.index_file}")
        except Exception as e:
            print(f" Error guardando índice: {e}")
    
    def _load_index(self) -> bool:
        """Carga el índice desde disco"""
        try:
            print(f" Cargando índice: {self.index_file}")
            
            # Cargar índice principal
            if not os.path.exists(self.index_file):
                print(f" Archivo de índice no encontrado: {self.index_file}")
                return False
            
            with open(self.index_file, 'rb') as f:
                self.inverted_index = pickle.load(f)
            
            # Cargar metadatos
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'rb') as f:
                    metadata = pickle.load(f)
                    self.document_metadata = metadata.get('document_metadata', {})
                    self.tfidf_calculator.document_norms = metadata.get('document_norms', {})
                    self.total_documents = metadata.get('total_documents', 0)
            
            print(f" Índice cargado: {len(self.inverted_index)} términos, {self.total_documents} documentos")
            return True
            
        except Exception as e:
            print(f" Error cargando índice: {e}")
            return False
    
    def _is_index_loaded(self) -> bool:
        """Verifica si el índice está cargado en memoria"""
        return len(self.inverted_index) > 0
    
    def _calculate_index_stats(self) -> Dict:
        """Calcula estadísticas del índice"""
        if not self.inverted_index:
            return {'status': 'Index not built'}
        
        total_postings = sum(len(postings) for postings in self.inverted_index.values())
        avg_postings_per_term = total_postings / len(self.inverted_index) if self.inverted_index else 0
        
        return {
            'total_terms': len(self.inverted_index),
            'total_documents': self.total_documents,
            'total_postings': total_postings,
            'avg_postings_per_term': avg_postings_per_term,
            'vocabulary_size': len(self.tfidf_calculator.vocabulary),
            'index_size_mb': self._get_index_size_mb(),
            'text_fields': self.text_fields,
            'language': self.language
        }
    
    def _get_index_size_mb(self) -> float:
        """Calcula el tamaño del índice en MB"""
        try:
            if os.path.exists(self.index_file):
                size_bytes = os.path.getsize(self.index_file)
                return size_bytes / (1024 * 1024)
        except:
            pass
        return 0.0
    
    def get_stats(self) -> Dict:
        """Retorna estadísticas del índice"""
        if os.path.exists(self.stats_file):
            try:
                with open(self.stats_file, 'rb') as f:
                    return pickle.load(f)
            except:
                pass
        
        if self._is_index_loaded():
            return self._calculate_index_stats()
        else:
            return {'status': 'Index not built'}


# Función helper para integración fácil
def create_text_index(data: List[dict], 
                     index_name: str, 
                     text_fields: List[str], 
                     language: str = 'spanish') -> Optional[InvertedIndex]:
    """
    Función helper para crear un índice invertido rápidamente
    
    Args:
        data: Lista de documentos
        index_name: Nombre del índice
        text_fields: Campos de texto a indexar
        language: Idioma para procesamiento
        
    Returns:
        Índice invertido construido o None si falla
    """
    try:
        index = InvertedIndex(index_name, text_fields, language)
        success = index.build_index_from_data(data)
        return index if success else None
    except Exception as e:
        print(f" Error creando índice: {e}")
        return None