"""
SPIMI: Single-Pass In-Memory Indexing Algorithm
Versión adaptada para el proyecto de base de datos multimedia con método load_csv
"""

import os
import pickle
import heapq
import tempfile
import csv
import math
from typing import Dict, List, Tuple, Iterator, Optional
from collections import defaultdict, Counter
import gc

# Imports con manejo de errores para compatibilidad
try:
    from text_processing.preprocessor import TextPreprocessor
    TEXT_PROCESSING_AVAILABLE = True
except ImportError:
    TEXT_PROCESSING_AVAILABLE = False

# Procesador de texto simplificado integrado
class SimpleTextProcessor:
    """Procesador de texto básico sin dependencias externas"""
    
    def __init__(self, language='spanish'):
        self.language = language
        self.setup_stopwords()
    
    def setup_stopwords(self):
        """Configurar stopwords básicas"""
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
        
        import re
        
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

class SimpleTFIDFCalculator:
    """Calculador TF-IDF básico integrado"""
    
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

class SPIMIBlock:
    """Representa un bloque parcial del índice invertido"""
    
    def __init__(self, block_id: int):
        self.block_id = block_id
        self.index = defaultdict(list)  # term -> [(doc_id, tf)]
        self.memory_usage = 0
        self.doc_count = 0
    
    def add_term(self, term: str, doc_id: int, tf: float):
        """Añade un término al bloque"""
        self.index[term].append((doc_id, tf))
        # Estimación aproximada del uso de memoria
        self.memory_usage += len(term) + 16  # aprox bytes por entrada
    
    def get_memory_usage_mb(self) -> float:
        """Retorna el uso de memoria en MB"""
        return self.memory_usage / (1024 * 1024)
    
    def sort_terms(self):
        """Ordena los términos alfabéticamente para el merge"""
        sorted_items = sorted(self.index.items())
        self.index = dict(sorted_items)

class SPIMIIndexBuilder:
    """Constructor de índice invertido usando SPIMI con método load_csv integrado"""
    
    def __init__(self, 
                 output_dir: str,
                 memory_limit_mb: float = 100,
                 temp_dir: Optional[str] = None,
                 text_fields: Optional[List[str]] = None,
                 language: str = 'spanish'):
        """
        Inicializa el constructor SPIMI
        
        Args:
            output_dir: Directorio donde guardar el índice final
            memory_limit_mb: Límite de memoria por bloque en MB
            temp_dir: Directorio temporal
            text_fields: Campos de texto a indexar
            language: Idioma para procesamiento
        """
        self.output_dir = output_dir
        self.memory_limit_mb = memory_limit_mb
        self.temp_dir = temp_dir or tempfile.mkdtemp()
        self.block_counter = 0
        self.block_files = []
        
        # Configuración para procesamiento de texto
        self.text_fields = text_fields or ['lyrics', 'track_name', 'track_artist']
        self.language = language
        
        # Inicializar procesador de texto
        if TEXT_PROCESSING_AVAILABLE:
            try:
                self.preprocessor = TextPreprocessor(language)
                print(" Usando procesador avanzado con NLTK")
            except:
                self.preprocessor = SimpleTextProcessor(language)
                print(" Fallback a procesador simplificado")
        else:
            self.preprocessor = SimpleTextProcessor(language)
            print(" Usando procesador simplificado integrado")
        
        self.tfidf_calculator = SimpleTFIDFCalculator()
        
        # Crear directorios si no existen
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
    
    def load_csv(self, csv_path: str, 
                 text_fields: Optional[List[str]] = None,
                 encoding: str = 'utf-8') -> str:
        """
        Carga un CSV y construye el índice invertido usando SPIMI
        Compatible con la interfaz de otros índices
        
        Args:
            csv_path: Ruta del archivo CSV
            text_fields: Campos de texto a indexar (opcional)
            encoding: Codificación del archivo
            
        Returns:
            Ruta del índice construido
        """
        print(f"\n SPIMI LOAD_CSV")
        print("=" * 50)
        print(f" Archivo: {csv_path}")
        
        # Usar campos de texto proporcionados o los por defecto
        if text_fields:
            self.text_fields = text_fields
        
        print(f" Campos de texto: {self.text_fields}")
        
        try:
            # Paso 1: Cargar datos del CSV
            data = []
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames) if reader.fieldnames else []
                print(f" Headers detectados: {headers}")
                
                # Verificar que los campos de texto existen
                available_text_fields = []
                for field in self.text_fields:
                    if field in headers:
                        available_text_fields.append(field)
                        print(f" Campo textual encontrado: {field}")
                    else:
                        print(f" Campo textual no encontrado: {field}")
                
                if not available_text_fields:
                    print(" No se encontraron campos textuales válidos")
                    return ""
                
                self.text_fields = available_text_fields
                
                # Cargar registros
                for row in reader:
                    data.append(row)
            
            print(f" Registros cargados: {len(data)}")
            
            # Paso 2: Preprocesar documentos
            processed_docs, doc_metadata = self._preprocess_documents(data)
            
            if not processed_docs:
                print(" No se procesaron documentos válidos")
                return ""
            
            # Paso 3: Calcular TF-IDF
            tfidf_vectors = self._calculate_tfidf_vectors(processed_docs)
            
            # Paso 4: Construir índice usando SPIMI
            index_path = self.build_index(processed_docs, tfidf_vectors)
            
            print(f" Índice SPIMI construido exitosamente: {index_path}")
            return index_path
            
        except Exception as e:
            print(f" Error en load_csv: {e}")
            import traceback
            traceback.print_exc()
            return ""
    
    def _preprocess_documents(self, data: List[dict]) -> Tuple[List[Tuple[int, List[str]]], Dict[int, dict]]:
        """Preprocesa los documentos concatenando campos de texto"""
        processed_docs = []
        doc_metadata = {}
        
        print(" Preprocesando documentos...")
        
        for doc_id, record in enumerate(data):
            if doc_id % 1000 == 0:
                print(f"   Procesando documento {doc_id + 1}/{len(data)}")
            
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
    
    def build_index(self, 
                   documents: List[Tuple[int, List[str]]], 
                   tfidf_vectors: List[Dict[str, float]]) -> str:
        """
        Construye el índice invertido usando SPIMI
        
        Args:
            documents: Lista de (doc_id, tokens)
            tfidf_vectors: Vectores TF-IDF precalculados
            
        Returns:
            Ruta del archivo de índice final
        """
        print(f" Construcción SPIMI: {len(documents)} documentos")
        print(f" Límite memoria: {self.memory_limit_mb} MB")
        
        # Paso 1: Crear bloques parciales
        self._create_partial_blocks(documents, tfidf_vectors)
        
        # Paso 2: Hacer merge de bloques
        final_index_path = self._merge_blocks()
        
        # Paso 3: Limpiar archivos temporales
        self._cleanup_temp_files()
        
        print(f" Índice SPIMI construido: {final_index_path}")
        return final_index_path
    
    def _create_partial_blocks(self, 
                              documents: List[Tuple[int, List[str]]], 
                              tfidf_vectors: List[Dict[str, float]]):
        """Crea bloques parciales del índice"""
        current_block = SPIMIBlock(self.block_counter)
        
        for i, (doc_id, tokens) in enumerate(documents):
            if i % 1000 == 0:
                print(f" Procesando documento {i+1}/{len(documents)}")
            
            # Obtener vector TF-IDF del documento
            tfidf_vector = tfidf_vectors[i] if i < len(tfidf_vectors) else {}
            
            # Añadir términos al bloque actual
            for term, tfidf_weight in tfidf_vector.items():
                current_block.add_term(term, doc_id, tfidf_weight)
            
            current_block.doc_count += 1
            
            # Verificar si se excede el límite de memoria
            if current_block.get_memory_usage_mb() >= self.memory_limit_mb:
                print(f" Bloque {current_block.block_id}: {current_block.get_memory_usage_mb():.1f} MB")
                self._write_block_to_disk(current_block)
                
                # Crear nuevo bloque
                self.block_counter += 1
                current_block = SPIMIBlock(self.block_counter)
                gc.collect()  # Garbage collection
        
        # Escribir el último bloque si tiene contenido
        if current_block.index:
            self._write_block_to_disk(current_block)
    
    def _write_block_to_disk(self, block: SPIMIBlock):
        """Escribe un bloque parcial al disco"""
        # Ordenar términos antes de escribir
        block.sort_terms()
        
        # Crear archivo del bloque
        block_filename = f"spimi_block_{block.block_id:04d}.pkl"
        block_path = os.path.join(self.temp_dir, block_filename)
        
        # Guardar bloque
        with open(block_path, 'wb') as f:
            pickle.dump({
                'block_id': block.block_id,
                'index': dict(block.index),
                'doc_count': block.doc_count,
                'term_count': len(block.index)
            }, f)
        
        self.block_files.append(block_path)
        print(f" Bloque {block.block_id}: {len(block.index)} términos, {block.doc_count} docs")
    
    def _merge_blocks(self) -> str:
        """Hace merge de todos los bloques parciales y precalcula normas"""
        print(f" Merging {len(self.block_files)} bloques...")
        
        if not self.block_files:
            # No hay bloques, crear índice vacío
            os.makedirs("embeddings", exist_ok=True)
            final_index_path = "embeddings/spimi_index.pkl"
            with open(final_index_path, 'wb') as f:
                pickle.dump({'index': {}, 'total_terms': 0, 'build_method': 'SPIMI'}, f)
            return final_index_path
        
        # Cargar todos los bloques
        all_blocks = []
        for block_file in self.block_files:
            try:
                with open(block_file, 'rb') as f:
                    block_data = pickle.load(f)
                    all_blocks.append(block_data['index'])
            except Exception as e:
                print(f" Error cargando bloque {block_file}: {e}")
                continue
        
        # Merge simple: combinar todos los índices
        final_index = {}
        for block_index in all_blocks:
            for term, postings in block_index.items():
                if term not in final_index:
                    final_index[term] = []
                final_index[term].extend(postings)
        
        # Consolidar postings duplicados por documento
        for term in final_index:
            # Agrupar por doc_id y sumar pesos
            doc_weights = defaultdict(float)
            for doc_id, weight in final_index[term]:
                doc_weights[doc_id] += weight
            
            # Convertir de vuelta a lista ordenada
            final_index[term] = [(doc_id, weight) for doc_id, weight in sorted(doc_weights.items())]
        
        # ========== NUEVA FUNCIONALIDAD: PRECALCULAR NORMAS ==========
        print(" Precalculando normas de documentos...")
        
        # Calcular document frequencies
        document_frequencies = {}
        total_documents = len(set(doc_id for postings in final_index.values() for doc_id, _ in postings))
        
        for term, postings in final_index.items():
            document_frequencies[term] = len(postings)
        
        # Crear vectores de documentos de manera eficiente
        doc_vectors = {}
        for term, postings in final_index.items():
            for doc_id, weight in postings:
                if doc_id not in doc_vectors:
                    doc_vectors[doc_id] = {}
                doc_vectors[doc_id][term] = weight
        
        # Calcular normas
        document_norms = {}
        for doc_id in range(total_documents):
            if doc_id in doc_vectors:
                vector = doc_vectors[doc_id]
                norm = math.sqrt(sum(weight ** 2 for weight in vector.values()))
                document_norms[doc_id] = norm
            else:
                document_norms[doc_id] = 0.0
        
        print(f" Normas precalculadas para {len(document_norms)} documentos")
        
        # Guardar índice final CON normas precalculadas
        os.makedirs("embeddings", exist_ok=True)
        final_index_path = "embeddings/spimi_index.pkl"
        with open(final_index_path, 'wb') as f:
            pickle.dump({
                'index': final_index,
                'total_terms': len(final_index),
                'total_documents': total_documents,
                'text_fields': self.text_fields,
                'language': self.language,
                'build_method': 'SPIMI',
                # ========== NUEVOS CAMPOS ==========
                'document_norms': document_norms,
                'document_frequencies': document_frequencies
            }, f)
        
        print(f" Merge completado: {len(final_index)} términos únicos")
        print(f" Normas guardadas: {len(document_norms)} documentos")
        return final_index_path


    def _cleanup_temp_files(self):
        """Limpia archivos temporales"""
        for block_file in self.block_files:
            try:
                if os.path.exists(block_file):
                    os.remove(block_file)
            except OSError:
                pass
        
        try:
            if os.path.exists(self.temp_dir):
                os.rmdir(self.temp_dir)
        except OSError:
            pass
    
    def get_stats(self) -> Dict:
        """Obtiene estadísticas del índice construido"""
        stats_file = "embeddings/spimi_index.pkl"
        if os.path.exists(stats_file):
            try:
                with open(stats_file, 'rb') as f:
                    data = pickle.load(f)
                    return {
                        'total_terms': data.get('total_terms', 0),
                        'total_documents': data.get('total_documents', 0),
                        'text_fields': data.get('text_fields', []),
                        'language': data.get('language', 'unknown'),
                        'build_method': 'SPIMI',
                        'index_size_mb': os.path.getsize(stats_file) / (1024 * 1024)
                    }
            except:
                pass
        return {'status': 'Index not built'}

# Función de demostración
def demo_spimi():
    """Demuestra el funcionamiento de SPIMI con load_csv"""
    print("=" * 50)
    print("DEMO ALGORITMO SPIMI CON LOAD_CSV")
    print("=" * 50)
    print("""
    SPIMI (Single-Pass In-Memory Indexing) integrado:
    
    1.  Método load_csv() compatible con otros índices
    2.  Procesamiento de texto integrado
    3.  Construcción por bloques en memoria secundaria
    4.  Merge eficiente de bloques parciales
    5.  Cálculo TF-IDF incorporado
    
    Uso:
    >>> spimi = SPIMIIndexBuilder('indices')
    >>> index_path = spimi.load_csv('datos/spotify_songs.csv')
    >>> stats = spimi.get_stats()
    
     Maneja datasets más grandes que la RAM
     Solo un pase sobre los documentos
     Compatible con la interfaz existente
    """)
    print("=" * 50)

if __name__ == "__main__":
    demo_spimi()