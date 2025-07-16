# engine.py - CORREGIDO para soporte de índices textuales SPIMI

import csv
import os
import pickle
from typing import List, Any, Tuple, Dict, Union, Optional
from indices.sequential import SequentialFile
from indices.isam import ISAM
from indices.hash_extensible import ExtendibleHash
from indices.btree import BPlusTree
from indices.base_index import BaseIndex

# Import condicional para evitar errores de importación
try:
    from indices.rtree import MultidimensionalRTree
    RTREE_AVAILABLE = True
except ImportError:
    print("Warning: R-Tree no disponible")
    MultidimensionalRTree = None
    RTREE_AVAILABLE = False

class Engine:
    def __init__(self):
        self.tables: Dict[str, BaseIndex] = {}
        self.table_headers: Dict[str, List[str]] = {}      
        self.table_file_paths: Dict[str, str] = {}
        self.table_schemas: Dict[str, List[tuple]] = {}
        
        # NUEVO: Soporte para índices textuales
        self.text_tables: Dict[str, Dict[str, Any]] = {}  # tabla -> {index_path, text_fields, csv_path}
        
        # NUEVO: Soporte para tablas de embeddings
        self.embedding_tables: Dict[str, Dict[str, Any]] = {}  # tabla -> {embeddings, metadata, pickle_path}

    def register_text_table(self, table_name: str, index_path: str, text_fields: List[str], csv_path: str):
        """Registra una tabla con índice textual SPIMI"""
        self.text_tables[table_name] = {
            'index_path': index_path,
            'text_fields': text_fields,
            'csv_path': csv_path,
            'type': 'SPIMI'
        }
        
        # También leer headers del CSV
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader, [])
                self.table_headers[table_name] = headers
                self.table_file_paths[table_name] = csv_path
                
            print(f" Tabla textual '{table_name}' registrada exitosamente")
            print(f"    CSV: {csv_path}")
            print(f"    Índice: {index_path}")
            print(f"    Campos textuales: {text_fields}")
            print(f"    Headers: {len(headers)} columnas")
            
        except Exception as e:
            print(f" Error leyendo headers para tabla {table_name}: {e}")

    def register_embedding_table(self, table_name: str, embeddings: Any, metadata: Dict[str, Any], pickle_path: str):
        """Registra una tabla de embeddings cargada desde pickle"""
        self.embedding_tables[table_name] = {
            'embeddings': embeddings,
            'metadata': metadata,
            'pickle_path': pickle_path,
            'type': 'embeddings'
        }
        
        # Crear headers basados en metadata
        if metadata and isinstance(metadata, dict):
            headers = ['embedding_id'] + list(next(iter(metadata.values())).keys() if metadata else [])
        else:
            headers = ['embedding_id', 'embedding_vector']
        
        self.table_headers[table_name] = headers
        self.table_file_paths[table_name] = pickle_path
        
        print(f" Tabla de embeddings '{table_name}' registrada exitosamente")
        print(f"    Pickle: {pickle_path}")
        print(f"    Embeddings shape: {embeddings.shape if hasattr(embeddings, 'shape') else 'unknown'}")
        print(f"    Headers: {headers}")

    def textual_search(self, table_name: str, query_text: str, k: int = 10) -> List[Tuple[Dict[str, Any], float]]:
        """
        Ejecuta búsqueda textual usando el índice SPIMI
        """
        print(f" Búsqueda textual en tabla '{table_name}'")
        print(f" Consulta: '{query_text}'")
        print(f" Top-K: {k}")
        
        # Verificar si es tabla textual
        if table_name not in self.text_tables:
            raise ValueError(f"Tabla '{table_name}' no tiene índice textual")
        
        text_info = self.text_tables[table_name]
        index_path = text_info['index_path']
        csv_path = text_info['csv_path']
        
        try:
            # Importar módulos necesarios
            from indices.spimi import SPIMIIndexBuilder
            from indices.inverted_index import InvertedIndex
            
            # Cargar el índice SPIMI construido
            if not os.path.exists(index_path):
                raise ValueError(f"Archivo de índice no encontrado: {index_path}")
            
            print(f" Cargando índice desde: {index_path}")
            
            # Cargar datos del índice
            with open(index_path, 'rb') as f:
                index_data = pickle.load(f)
            
            print(f" Índice cargado: {len(index_data.get('index', {}))} términos")
            
            # Cargar documentos originales para los resultados
            documents = []
            
            # Si hay CSV, cargar desde ahí
            if csv_path and os.path.exists(csv_path):
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        documents.append(row)
                print(f" Documentos cargados desde CSV: {len(documents)}")
            
            # Si no hay CSV, intentar cargar desde el índice
            elif 'doc_info' in index_data:
                doc_info = index_data['doc_info']
                print(f" Documentos encontrados en índice: {len(doc_info)}")
                # Convertir doc_info a lista de documentos
                for doc_id, doc_data in doc_info.items():
                    if isinstance(doc_data, dict):
                        documents.append(doc_data)
                    else:
                        # Si es solo texto, crear un documento básico
                        documents.append({'content': str(doc_data), 'doc_id': doc_id})
            
            # Si no hay documentos, crear documentos vacíos
            if not documents:
                print(" ADVERTENCIA: No se encontraron documentos, usando placeholders")
                # Crear documentos placeholder basados en el índice
                doc_count = index_data.get('total_documents', 100)
                for i in range(doc_count):
                    documents.append({'doc_id': i, 'content': f'Documento {i}'})
            
            # Procesar consulta usando componentes de InvertedIndex
            # Crear un índice temporal para ejecutar la búsqueda
            temp_index = InvertedIndex(table_name, text_info['text_fields'], 'spanish')
            
            # Cargar la información del índice SPIMI
            temp_index.inverted_index = index_data.get('index', {})
            temp_index.total_documents = index_data.get('total_documents', len(documents))
            temp_index.document_metadata = {i: doc for i, doc in enumerate(documents)}
            
            # OPTIMIZACIÓN: Cargar normas precalculadas si existen
            if 'document_norms' in index_data:
                print(" Cargando normas precalculadas...")
                temp_index.tfidf_calculator.document_norms = index_data['document_norms']
                temp_index.tfidf_calculator.document_count = temp_index.total_documents
                temp_index.tfidf_calculator.vocabulary = set(temp_index.inverted_index.keys())
                
                # Cargar document frequencies
                temp_index.tfidf_calculator.document_frequencies = index_data.get('document_frequencies', {})
                
                print(f" Configuración TF-IDF cargada desde índice")
            else:
                print(" Normas no encontradas en índice, calculando...")
                # Fallback al método optimizado anterior
                self._calculate_document_norms_optimized(temp_index)
            
            # Ejecutar búsqueda
            results = temp_index.search(query_text, k)
            
            print(f" Búsqueda completada: {len(results)} resultados encontrados")
            
            return results
            
        except Exception as e:
            print(f" Error en búsqueda textual: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _calculate_document_norms_optimized(self, temp_index):
        """Calcula normas de documentos de manera optimizada"""
        print(" Calculando normas de documentos (método optimizado)...")
        
        # Configurar TF-IDF calculator básico
        temp_index.tfidf_calculator.document_count = temp_index.total_documents
        temp_index.tfidf_calculator.vocabulary = set(temp_index.inverted_index.keys())
        
        # Reconstruir document frequencies
        for term, postings in temp_index.inverted_index.items():
            temp_index.tfidf_calculator.document_frequencies[term] = len(postings)
        
        # Inicializar normas
        temp_index.tfidf_calculator.document_norms = {}
        doc_vectors = {}  # doc_id -> {term: weight}
        
        # Una sola pasada por el índice invertido
        for term, postings in temp_index.inverted_index.items():
            for post_doc_id, weight in postings:
                if post_doc_id not in doc_vectors:
                    doc_vectors[post_doc_id] = {}
                doc_vectors[post_doc_id][term] = weight
        
        # Calcular normas
        import math
        for doc_id in range(temp_index.total_documents):
            if doc_id in doc_vectors:
                vector = doc_vectors[doc_id]
                norm = math.sqrt(sum(weight ** 2 for weight in vector.values()))
                temp_index.tfidf_calculator.document_norms[doc_id] = norm
            else:
                temp_index.tfidf_calculator.document_norms[doc_id] = 0.0
        
        print(f" Normas calculadas para {len(temp_index.tfidf_calculator.document_norms)} documentos")

    def _init_index(self, tipo: str, table: str, index_field: int, schema: Optional[List[Tuple[str, str, int]]]) -> BaseIndex:
        """Inicializa un índice según su tipo"""
        if tipo == 'sequential':
            return SequentialFile(f'{table}_data.bin',
                                f'{table}_aux.bin',
                                field_index=index_field)
        elif tipo == 'isam':
            if schema is None:
                raise ValueError("ISAM requiere un schema válido")
            return ISAM(f'{table}_data.bin',
                        f'{table}_index.bin',
                        schema,
                        index_field)
        elif tipo == 'hash':
            hash_idx = ExtendibleHash(
                dir_file=f'indices/{table}_hash_dir.pkl',
                data_file=f'indices/{table}_hash_data.bin'
            )
            hash_idx.field_index = index_field
            return hash_idx
        elif tipo == 'bplustree':
            tree = BPlusTree(f'{table}_btree.pkl')
            tree.field_index = index_field
            return tree
        elif tipo == 'rtree':
            if not RTREE_AVAILABLE or MultidimensionalRTree is None:
                raise ValueError("R-Tree no está disponible. Verifica la instalación de la librería rtree.")
            return MultidimensionalRTree(path=f'{table}_rtree', dimension=2)
        else:
            raise ValueError(f"Tipo de índice '{tipo}' no soportado")

    def load_csv(self, table: str, path: str, tipo: str, index_field: int) -> str:
        """Carga un archivo CSV en una tabla con el índice especificado"""
        print(f"Intentando leer archivo: {path}")

        # Leer y guardar headers
        try:
            headers = []
            with open(path, 'r', encoding='latin1') as f:
                reader = csv.reader(f)
                first_row = next(reader, [])
                headers = [col.strip() for col in first_row if col.strip()]
            
            self.table_headers[table] = headers
            self.table_file_paths[table] = path
            
            print(f"Headers detectados para tabla '{table}': {headers}")
            print(f"Total de columnas: {len(headers)}")
            
        except Exception as e:
            print(f"Advertencia: No se pudieron leer headers del archivo {path}: {e}")
            self.table_headers[table] = []
            self.table_file_paths[table] = path

        # Crear índice según tipo
        if tipo == 'rtree':
            # R-Tree espacial
            if not RTREE_AVAILABLE or MultidimensionalRTree is None:
                raise ValueError("R-Tree no está disponible")
            
            idx = MultidimensionalRTree(path=f'{table}_rtree', dimension=2)
            idx.load_csv(path)  # MultidimensionalRTree.load_csv acepta str

        elif tipo == 'isam':
            # ISAM requiere un manejo especial con diccionarios
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.reader(f)
                headers_isam = next(reader)
                rows = list(reader)

            # Crear schema ANTES de inicializar el índice
            schema = [(f'col{i}', '20s', 20) for i in range(len(headers_isam))]
            self.table_schemas[table] = schema
            
            # Convertir filas a diccionarios para ISAM
            data_dicts = [dict(zip([f'col{i}' for i in range(len(row))], row)) for row in rows]
            
            # Ahora schema es definitivamente no-None
            idx = self._init_index(tipo, table, index_field, schema)
            # ISAM.load_csv acepta específicamente List[Dict[str, Any]]
            if isinstance(idx, ISAM):
                idx.load_csv(data_dicts)  # type: ignore[arg-type]
            else:
                # Fallback para otros tipos que puedan implementar load_csv
                for row in rows:
                    idx.insert(None, row)

        elif tipo == 'hash':
            # Hash Extensible requiere configuración específica
            idx = self._init_index(tipo, table, index_field, None)  # Schema explícitamente None
            if isinstance(idx, ExtendibleHash):
                idx.field_index = index_field
                idx.load_csv(path)  # ExtendibleHash.load_csv acepta str
            else:
                raise ValueError("Error inicializando Hash Extensible")

        elif tipo == 'bplustree':
            # B+ Tree requiere configuración específica
            idx = self._init_index(tipo, table, index_field, None)  # Schema explícitamente None
            if isinstance(idx, BPlusTree):
                idx.field_index = index_field
                idx.load_csv(path)  # BPlusTree.load_csv acepta str
            else:
                raise ValueError("Error inicializando B+ Tree")

        else:
            # Sequential File y otros índices
            idx = self._init_index(tipo, table, index_field, None)  # Schema explícitamente None
            
            if isinstance(idx, SequentialFile):
                # Sequential File acepta path directamente
                idx.load_csv(path)  # SequentialFile.load_csv acepta str
            else:
                # Para otros índices, cargar datos manualmente
                with open(path, newline='', encoding='latin1') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                
                for row in rows:
                    idx.insert(None, row)

        # Guardar índice en tabla
        self.tables[table] = idx
        
        headers_count = len(self.table_headers.get(table, []))
        tipo_real = type(idx).__name__
        
        extra_info = ""
        if isinstance(idx, ExtendibleHash):
            if hasattr(idx, 'get_stats'):
                stats = idx.get_stats()
                extra_info = f" (Profundidad global: {stats['global_depth']}, {stats['total_records']} registros, {stats['total_buckets']} buckets)"
        elif RTREE_AVAILABLE and MultidimensionalRTree and isinstance(idx, MultidimensionalRTree):
            records_loaded = getattr(idx, 'data_map', {})
            if isinstance(records_loaded, dict):
                extra_info = f" ({len(records_loaded)} registros espaciales cargados)"
        
        return f"Tabla '{table}' cargada con éxito usando índice {tipo} ({tipo_real}){extra_info}. Detectadas {headers_count} columnas: {', '.join(self.table_headers.get(table, [])[:5])}{'...' if headers_count > 5 else ''}"

    # ========== MÉTODOS PARA HEADERS ==========
    
    def get_table_headers(self, table_name: str) -> List[str]:
        """Obtener los headers/columnas de una tabla específica"""
        return self.table_headers.get(table_name, [])
    
    def get_table_file_path(self, table_name: str) -> str:
        """Obtener la ruta del archivo CSV original de una tabla"""
        return self.table_file_paths.get(table_name, '')
    
    def get_table_info(self, table_name: str) -> dict:
        """Obtener información completa de una tabla"""
        info = {
            'name': table_name,
            'headers': self.get_table_headers(table_name),
            'csv_path': self.get_table_file_path(table_name),
            'headers_count': len(self.get_table_headers(table_name))
        }
        
        # Calcular record_count
        record_count = 0
        csv_path = self.get_table_file_path(table_name)
        if csv_path and os.path.exists(csv_path):
            try:
                import pandas as pd
                df = pd.read_csv(csv_path)
                record_count = len(df)
            except Exception as e:
                print(f"Error calculando record_count para {table_name}: {e}")
                # Fallback: contar líneas del archivo
                try:
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        record_count = sum(1 for line in f) - 1  # -1 para excluir header
                except:
                    record_count = 0
        
        info['record_count'] = record_count
        
        # Información específica para tablas textuales
        if table_name in self.text_tables:
            text_info = self.text_tables[table_name]
            info.update({
                'index_type': 'SPIMI_TextIndex',
                'text_fields': text_info['text_fields'],
                'index_path': text_info['index_path']
            })
        elif table_name in self.tables:
            index = self.tables[table_name]
            info.update({
                'index_type': type(index).__name__,
                'field_index': getattr(index, 'field_index', None)
            })
            
            if isinstance(index, ExtendibleHash) and hasattr(index, 'get_stats'):
                info['hash_stats'] = index.get_stats()
        
        return info
    
    def list_all_tables_info(self) -> Dict[str, dict]:
        """Obtener información de todas las tablas cargadas"""
        all_tables = set(self.tables.keys()) | set(self.text_tables.keys())
        return {table_name: self.get_table_info(table_name) for table_name in all_tables}

    # ========== MÉTODOS PRINCIPALES ==========

    def _format_record_to_csv(self, record: Any) -> str:
        """Convierte cualquier registro a formato CSV"""
        if isinstance(record, dict):
            values = [str(v) for v in record.values()]
        elif isinstance(record, (list, tuple)):
            values = [str(v) for v in record]
        elif isinstance(record, str):
            if '|' in record:
                values = [v.strip() for v in record.split('|')]
            else:
                values = [record]
        else:
            values = [str(record)]
        
        # Limpiar y formatear cada valor
        cleaned_values = []
        for v in values:
            cleaned = str(v).strip()
            if ',' in cleaned or '"' in cleaned or '\n' in cleaned:
                cleaned = '"' + cleaned.replace('"', '""') + '"'
            cleaned_values.append(cleaned)
        
        return ','.join(cleaned_values)

    def _list_to_isam_dict(self, table: str, values: List[str]) -> Dict[str, Any]:
        """Convierte una lista de valores a diccionario para ISAM"""
        if table not in self.table_schemas:
            schema = [(f'col{i}', '20s', 20) for i in range(len(values))]
            self.table_schemas[table] = schema
        else:
            schema = self.table_schemas[table]
        
        result = {}
        for i, (field_name, _, _) in enumerate(schema):
            if i < len(values):
                result[field_name] = values[i]
            else:
                result[field_name] = ""
        
        return result

    def insert(self, table: str, values: List[str]) -> str:
        """Insertar un registro en una tabla"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        idx = self.tables[table]
        
        if isinstance(idx, ISAM):
            val_dict = self._list_to_isam_dict(table, values)
            idx.insert(None, val_dict)
        else:
            idx.insert(None, values)
        
        return f"Registro insertado en '{table}'"

    def scan(self, table: str) -> str:
        """Escanear tabla completa"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        idx = self.tables[table]
        registros = idx.scan_all()
        
        if RTREE_AVAILABLE and MultidimensionalRTree and isinstance(idx, MultidimensionalRTree):
            formatted_records = []
            for vector, obj in registros:
                if isinstance(obj, list):
                    cleaned_values = []
                    for v in obj:
                        cleaned = str(v).strip()
                        if ',' in cleaned or '"' in cleaned or '\n' in cleaned:
                            cleaned = '"' + cleaned.replace('"', '""') + '"'
                        cleaned_values.append(cleaned)
                    csv_record = ','.join(cleaned_values)
                    formatted_records.append(csv_record)
                else:
                    csv_record = self._format_record_to_csv(obj)
                    formatted_records.append(csv_record)
            return '\n'.join(formatted_records)
        
        formatted_records = []
        for record in registros:
            csv_record = self._format_record_to_csv(record)
            formatted_records.append(csv_record)
        
        return '\n'.join(formatted_records)

    def search(self, table: str, key: str, column: int) -> List[str]:
        """Buscar registros básicos (solo para índices tradicionales)"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        idx = self.tables[table]
        
        # Para hash con búsqueda directa
        if (hasattr(idx, 'search') and hasattr(idx, 'field_index') and 
            idx.field_index == column and isinstance(idx, ExtendibleHash)):
            try:
                resultados = idx.search(key)
                final_result = []
                for r in resultados:
                    csv_record = self._format_record_to_csv(r)
                    final_result.append(csv_record)
                return final_result
            except Exception as e:
                print(f"Error en búsqueda directa: {e}")
        
        # Full scan con filtro manual para otros casos
        resultados = []
        all_records = idx.scan_all()
        
        for row in all_records:
            try:
                cell_value = None
                
                if isinstance(row, dict):
                    values = list(row.values())
                    if column < len(values):
                        cell_value = str(values[column]).strip()
                elif isinstance(row, (list, tuple)):
                    if column < len(row):
                        cell_value = str(row[column]).strip()
                elif isinstance(row, str):
                    if '|' in row:
                        cols = [c.strip() for c in row.split('|')]
                        if column < len(cols):
                            cell_value = cols[column].strip()
                    else:
                        import csv
                        import io
                        try:
                            reader = csv.reader(io.StringIO(row.strip()))
                            cols = next(reader, [])
                            if column < len(cols):
                                cell_value = cols[column].strip()
                        except:
                            cols = [c.strip() for c in row.split(',')]
                            if column < len(cols):
                                cell_value = cols[column].strip()
                
                if cell_value is not None and cell_value == str(key).strip():
                    csv_record = self._format_record_to_csv(row)
                    resultados.append(csv_record)
                    
            except Exception:
                continue

        return resultados

    def range_search(self, table: str, begin_key: str, end_key: str) -> List[str]:
        """Búsqueda por rango básica"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        idx = self.tables[table]

        # Manejo específico para R-Tree
        if RTREE_AVAILABLE and MultidimensionalRTree and isinstance(idx, MultidimensionalRTree):
            try:
                point = [float(x.strip()) for x in begin_key.split(',')]
                
                try:
                    if '.' in str(end_key):
                        param = float(end_key)
                    else:
                        param = int(end_key)
                except ValueError:
                    raise ValueError("Parámetro inválido para R-Tree")
                
                spatial_results = idx.range_search(point, param)
                
                formatted_results = []
                for dist, obj in spatial_results:
                    if isinstance(obj, list):
                        cleaned_values = []
                        for v in obj:
                            cleaned = str(v).strip()
                            if ',' in cleaned or '"' in cleaned or '\n' in cleaned:
                                cleaned = '"' + cleaned.replace('"', '""') + '"'
                            cleaned_values.append(cleaned)
                        
                        csv_record = ','.join(cleaned_values) + f',{dist:.3f}'
                        formatted_results.append(csv_record)
                    else:
                        csv_record = self._format_record_to_csv(obj)
                        formatted_results.append(f"{csv_record},{dist:.3f}")
                
                return formatted_results
                
            except Exception as e:
                raise ValueError(f"Error en búsqueda espacial R-Tree: {e}")
        
        if isinstance(idx, ExtendibleHash):
            raise ValueError("Hash Extensible no soporta búsquedas por rango. Use ISAM o B+ Tree para rangos.")
        
        if hasattr(idx, 'range_search'):
            raw_results = idx.range_search(begin_key, end_key)
            return [self._format_record_to_csv(r) for r in raw_results]
        
        return []

    def remove(self, table: str, key: str) -> List[str]:
        """Eliminar registros básicos"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        idx = self.tables[table]
        
        if not hasattr(idx, 'remove'):
            raise NotImplementedError(f"El índice {type(idx).__name__} no soporta eliminación")
        
        try:
            raw_results = idx.remove(key)
            
            if not raw_results:
                return []
            
            formatted_results = []
            
            if isinstance(idx, ExtendibleHash):
                formatted_results = raw_results if isinstance(raw_results, list) else [str(raw_results)]
            elif hasattr(idx, '__class__') and 'SequentialFile' in str(type(idx)):
                for r in raw_results:
                    if isinstance(r, str) and '|' in r:
                        cols = [c.strip() for c in r.split('|')]
                        csv_record = ','.join(f'"{c}"' if ',' in c else c for c in cols)
                        formatted_results.append(csv_record)
                    else:
                        formatted_results.append(self._format_record_to_csv(r))
            else:
                for r in raw_results:
                    csv_record = self._format_record_to_csv(r)
                    formatted_results.append(csv_record)
            
            return formatted_results
            
        except Exception as e:
            print(f"Error eliminando registros: {e}")
            return []