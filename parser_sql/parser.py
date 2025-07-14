# parser_sql/parser.py - VERSIÓN CORREGIDA PARA DETECTAR audio_path

import re
import os
import random
import string
import time
import hashlib
from typing import List, Dict, Any, Optional, Tuple

class SQLParser:
    def __init__(self, engine):
        self.engine = engine
        self.text_indices = {}  # Almacenar referencias a índices textuales
        self.multimedia_engines = {}  # Almacenar motores multimedia por tabla
        
    def register_text_index(self, table_name: str, text_index):
        """Registra un índice textual para una tabla"""
        self.text_indices[table_name] = text_index
        print(f" Índice textual registrado para tabla: {table_name}")
    
    def register_multimedia_engine(self, table_name: str, multimedia_engine):
        """Registra un motor multimedia para una tabla"""
        self.multimedia_engines[table_name] = multimedia_engine
        print(f" Motor multimedia registrado para tabla: {table_name}")
        
    def parse_and_execute(self, query: str) -> Any:
        """
        Parsea y ejecuta una consulta SQL (incluye texto, multimedia y tradicional)
        """
        query = query.strip().rstrip(';')
        # Normalizar espacios en blanco (eliminar saltos de línea y espacios múltiples)
        query = ' '.join(query.split())
        query_lower = query.lower()
        
        print(f" DEBUG PARSER: Query normalizada: '{query}'")
        print(f" DEBUG PARSER: Contiene <->? {' <-> ' in query}")
        print(f" DEBUG PARSER: Contiene @@? {' @@ ' in query}")
        
        # Verificar consultas especiales primero (antes de SELECT genérico)
        if ' <-> ' in query:
            print(" DEBUG PARSER: Enviando a _parse_multimedia_search")
            return self._parse_multimedia_search(query)
        elif ' @@ ' in query:
            print(" DEBUG PARSER: Enviando a _parse_textual_search")
            return self._parse_textual_search(query)
        elif query_lower.startswith('create multimedia'):
            return self._parse_create_multimedia_table(query)
        elif query_lower.startswith('create table'):
            return self._parse_create_table(query)
        elif query_lower.startswith('select'):
            print(" DEBUG PARSER: Enviando a _parse_select (SELECT genérico)")
            return self._parse_select(query)
        elif query_lower.startswith('insert'):
            if 'generate_series' in query_lower or 'generate_data' in query_lower:
                return self._parse_insert_generate(query)
            else:
                return self._parse_insert(query)
        elif query_lower.startswith('delete'):
            return self._parse_delete(query)
        else:
            raise ValueError(f"Tipo de consulta no soportado: {query}")

    # ==================== NUEVOS MÉTODOS PARA MULTIMEDIA ====================
    
    def _parse_create_multimedia_table(self, query: str) -> str:
        """
        Parsea CREATE MULTIMEDIA TABLE
        Sintaxis: CREATE MULTIMEDIA TABLE tabla_name FROM FILE "archivo.csv" 
                 USING media_type (image|audio) WITH method (sift|resnet50|mfcc) CLUSTERS n;
        """
        pattern = r'create\s+multimedia\s+table\s+(\w+)\s+from\s+file\s+"([^"]+)"\s+using\s+(image|audio)\s+with\s+method\s+(\w+)(?:\s+clusters\s+(\d+))?'
        match = re.search(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Sintaxis incorrecta para CREATE MULTIMEDIA TABLE")
        
        table_name = match.group(1)
        file_path = match.group(2)
        media_type = match.group(3).lower()
        method = match.group(4).lower()
        n_clusters = int(match.group(5)) if match.group(5) else 256
        
        print(f" Creando tabla multimedia: {table_name}")
        print(f" Archivo: {file_path}")
        print(f" Tipo de media: {media_type}")
        print(f" Método: {method}")
        print(f" Clusters: {n_clusters}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        return self._create_multimedia_system(table_name, file_path, media_type, method, n_clusters)
    
    def _create_multimedia_system(self, table_name: str, file_path: str, 
                                media_type: str, method: str, n_clusters: int) -> str:
        """Crea un sistema multimedia completo"""
        try:
            # Importar motor multimedia
            from multimedia.multimedia_engine import MultimediaEngine
            import pandas as pd
            
            # Crear motor multimedia
            engine = MultimediaEngine(
                media_type=media_type,
                feature_method=method,
                n_clusters=n_clusters
            )
            
            print(f" Motor multimedia inicializado: {media_type} - {method}")
            
            # Cargar dataset
            df = pd.read_csv(file_path)
            print(f" Dataset cargado: {len(df)} registros")
            
            # CORREGIDO: Detectar columna de archivos multimedia con prioridades específicas
            path_column = None
            
            # Lista de prioridades para columnas de archivos multimedia
            priority_columns = ['audio_path', 'image_path', 'file_path', 'path']
            
            # Buscar columnas prioritarias primero
            for col in priority_columns:
                if col in df.columns:
                    path_column = col
                    break
            
            # Si no encuentra las prioritarias, buscar cualquier columna que contenga las palabras clave
            if not path_column:
                path_candidates = []
                for col in df.columns:
                    col_lower = col.lower()
                    if 'path' in col_lower or 'file' in col_lower:
                        path_candidates.append(col)
                
                if path_candidates:
                    path_column = path_candidates[0]
            
            # Si aún no encuentra, mostrar error detallado
            if not path_column:
                raise ValueError(f"No se encontró columna de rutas de archivos multimedia.\nColumnas disponibles: {list(df.columns)}\nSe esperaba alguna de: {priority_columns}")
            
            print(f" Columna de archivos detectada: {path_column}")
            
            # Verificar que la columna contiene rutas válidas
            sample_paths = df[path_column].head(3).tolist()
            print(f" Rutas de muestra:")
            valid_samples = 0
            for i, sample_path in enumerate(sample_paths, 1):
                if pd.notna(sample_path) and os.path.exists(sample_path):
                    print(f"   {i}.  {os.path.basename(sample_path)}")
                    valid_samples += 1
                else:
                    print(f"   {i}.  {sample_path} (no existe)")
            
            if valid_samples == 0:
                raise ValueError(f"Ningún archivo de muestra existe. Verifica las rutas en la columna '{path_column}'")
            
            # === CONSTRUCCIÓN DEL SISTEMA MULTIMEDIA ===
            
            print("\n=== CONSTRUYENDO SISTEMA MULTIMEDIA ===")
            
            # 1. Extraer características
            print("\n1. Extrayendo características...")
            start_time = time.time()
            features_data = engine.extract_features_from_dataframe(
                df=df,
                path_column=path_column,
                base_path='',
                save_features=True,
                features_path=f'multimedia_data/{table_name}_features.pkl'
            )
            extraction_time = time.time() - start_time
            
            if not features_data:
                raise ValueError("No se pudieron extraer características")
            
            print(f" Características extraídas: {len(features_data)} archivos ({extraction_time:.2f}s)")
            
            # 2. Construir codebook
            print("\n2. Construyendo diccionario...")
            start_time = time.time()
            engine.build_codebook(
                save_codebook=True,
                codebook_path=f'multimedia_data/{table_name}_codebook.pkl'
            )
            codebook_time = time.time() - start_time
            print(f" Codebook construido ({codebook_time:.2f}s)")
            
            # 3. Crear histogramas
            print("\n3. Creando histogramas...")
            start_time = time.time()
            engine.create_histograms(
                save_histograms=True,
                histograms_path=f'multimedia_data/{table_name}_histograms.pkl'
            )
            histograms_time = time.time() - start_time
            print(f" Histogramas creados ({histograms_time:.2f}s)")
            
            # 4. Construir índices
            print("\n4. Construyendo índices de búsqueda...")
            start_time = time.time()
            engine.build_search_indices()
            indices_time = time.time() - start_time
            print(f" Índices construidos ({indices_time:.2f}s)")
            
            # 5. Guardar sistema completo
            print("\n5. Guardando sistema...")
            engine.save_complete_system(f'multimedia_data/{table_name}_system')
            
            # Registrar motor en el parser
            self.register_multimedia_engine(table_name, engine)
            
            # También registrar en el engine tradicional para mantener metadatos
            self.engine.tables[table_name] = f"multimedia_{media_type}_{method}"
            self.engine.table_headers[table_name] = list(df.columns)
            self.engine.table_file_paths[table_name] = file_path
            
            total_time = extraction_time + codebook_time + histograms_time + indices_time
            
            print(f"\n SISTEMA MULTIMEDIA CREADO EXITOSAMENTE")
            print(f" Tiempo total: {total_time:.2f}s")
            print(f" Estadísticas:")
            print(f"   - Características: {len(features_data)} archivos")
            print(f"   - Vocabulario: {n_clusters} visual/acoustic words")
            print(f"   - Histogramas: {len(engine.histograms_data)} objetos")
            
            return f"Tabla multimedia '{table_name}' creada exitosamente. Tipo: {media_type}, Método: {method}, Clusters: {n_clusters}"
            
        except Exception as e:
            print(f" Error creando sistema multimedia: {e}")
            import traceback
            traceback.print_exc()
            raise ValueError(f"Error creando tabla multimedia: {e}")
    
    def _parse_multimedia_search(self, query: str) -> Dict[str, Any]:
        """
        Parsea consultas multimedia con operador <->
        Sintaxis: SELECT campos FROM tabla WHERE campo_sim <-> 'ruta_archivo' [METHOD método] LIMIT k;
        """
        query_clean = query.strip().rstrip(';')
        
        # Patrones para capturar consultas multimedia (más flexibles)
        patterns = [
            # Con método específico y LIMIT
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+<->\s*["\']([^"\']+)["\']\s+METHOD\s+(\w+)\s+LIMIT\s+(\d+)',
            # Con método específico sin LIMIT
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+<->\s*["\']([^"\']+)["\']\s+METHOD\s+(\w+)',
            # Sin método específico con LIMIT
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+<->\s*["\']([^"\']+)["\']\s+LIMIT\s+(\d+)',
            # Sin método específico sin LIMIT
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+<->\s*["\']([^"\']+)["\']',
        ]
        
        print(f" DEBUG: Parseando consulta multimedia: {query_clean}")
        print(f" DEBUG: Patrones a probar: {len(patterns)}")
        
        parsed_query = None
        
        for i, pattern in enumerate(patterns):
            print(f" DEBUG: Probando patrón {i+1}: {pattern}")
            match = re.search(pattern, query_clean, re.IGNORECASE | re.DOTALL)
            if match:
                print(f" DEBUG: ¡Patrón {i+1} coincidió! Grupos: {match.groups()}")
                fields_str = match.group(1).strip()
                table = match.group(2).strip()
                similarity_field = match.group(3).strip()
                query_file = match.group(4).strip()
                
                if i < 2:  # Patrones con método
                    method = match.group(5).strip()
                    limit = int(match.group(6)) if len(match.groups()) >= 6 and match.group(6) else 10
                else:  # Patrones sin método
                    method = 'inverted'  # Método por defecto
                    limit = int(match.group(5)) if len(match.groups()) >= 5 and match.group(5) else 10
                
                # Procesar campos
                if fields_str.strip() == '*':
                    fields = ['*']
                else:
                    fields = [f.strip() for f in fields_str.split(',')]
                
                parsed_query = {
                    'fields': fields,
                    'table': table,
                    'similarity_field': similarity_field,
                    'query_file': query_file,
                    'method': method,
                    'limit': limit,
                    'original_sql': query
                }
                break
        
        if not parsed_query:
            print(f" DEBUG: Ningún patrón coincidió para: {query_clean}")
            raise ValueError("Sintaxis de consulta multimedia no válida. Use: SELECT campos FROM tabla WHERE campo_sim <-> 'archivo' [METHOD método] LIMIT k;")
        
        # Ejecutar búsqueda multimedia
        return self._execute_multimedia_query(parsed_query)
    
    def _execute_multimedia_query(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Ejecuta una consulta multimedia y retorna resultados detallados"""
        table_name = parsed['table']
        query_file = parsed['query_file']
        method = parsed['method'].lower()
        k = parsed['limit']
        fields = parsed['fields']
        
        print(f"\n EJECUTANDO BÚSQUEDA MULTIMEDIA")
        print("=" * 60)
        print(f" Archivo de consulta: {os.path.basename(query_file)}")
        print(f" Tabla: {table_name}")
        print(f" Método: {method.upper()}")
        print(f" Top-K: {k}")
        print(f"Campos solicitados: {fields}")
        
        # Verificar que existe el motor multimedia
        if table_name not in self.multimedia_engines:
            raise ValueError(f"Tabla '{table_name}' no tiene motor multimedia configurado")
        
        if not os.path.exists(query_file):
            raise FileNotFoundError(f"Archivo de consulta no encontrado: {query_file}")
        
        engine = self.multimedia_engines[table_name]
        
        try:
            # === EJECUTAR BÚSQUEDA ===
            print(f"\n Buscando archivos similares...")
            
            start_time = time.time()
            results = engine.search_similar(query_file, k=k, method=method)
            search_time = time.time() - start_time
            
            if not results:
                print(f" No se encontraron resultados similares")
                return {
                    'results': [],
                    'execution_time': search_time,
                    'query_info': parsed,
                    'stats': {}
                }
            
            print(f" Búsqueda completada en {search_time:.4f} segundos")
            print(f" Resultados encontrados: {len(results)}")
            
            # === CARGAR METADATOS ===
            print(f"\n Cargando metadatos...")
            metadata = self._load_metadata_for_multimedia(table_name, results, fields)
            
            # === FORMATEAR RESULTADOS ===
            formatted_results = []
            
            print(f"\n RESULTADOS DE BÚSQUEDA:")
            print("-" * 60)
            
            for i, (file_path, similarity) in enumerate(results, 1):
                filename = os.path.basename(file_path)
                
                # Obtener metadatos para este archivo
                file_metadata = metadata.get(file_path, {})
                
                # Mostrar resultado en consola
                title = file_metadata.get('title', file_metadata.get('name', filename))
                print(f"{i:2d}. [{similarity:.4f}] {title}")
                print(f"     {filename}")
                
                # Crear registro estructurado
                result_record = {
                    'rank': i,
                    'file_path': file_path,
                    'filename': filename,
                    'similarity': round(similarity, 4),
                    'metadata': file_metadata
                }
                
                # Crear CSV si se solicita
                if fields != ['*']:
                    csv_values = []
                    for field in fields:
                        if field == 'similarity':
                            csv_values.append(str(round(similarity, 4)))
                        elif field == 'filename':
                            csv_values.append(filename)
                        elif field == 'file_path':
                            csv_values.append(file_path)
                        else:
                            value = file_metadata.get(field, '')
                            # Escapar para CSV
                            if ',' in str(value) or '"' in str(value):
                                value = f'"{str(value).replace(chr(34), chr(34)*2)}"'
                            csv_values.append(str(value))
                    
                    result_record['csv'] = ','.join(csv_values)
                
                formatted_results.append(result_record)
            
            # === ESTADÍSTICAS ===
            stats = engine.get_system_statistics()
            
            print(f"\nESTADÍSTICAS DE RENDIMIENTO:")
            print(f"     Tiempo de ejecución: {search_time:.4f} segundos")
            print(f"    Método utilizado: {method.upper()}")
            print(f"    Documentos en base: {stats.get('histograms_created', 0)}")
            if method == 'inverted':
                inv_stats = stats.get('inverted_search', {})
                print(f"   Términos indexados: {inv_stats.get('terms_in_index', 0)}")
                print(f"    Postings totales: {inv_stats.get('total_postings', 0)}")
            
            return {
                'results': formatted_results,
                'execution_time': search_time,
                'query_info': parsed,
                'stats': stats,
                'total_found': len(results)
            }
            
        except Exception as e:
            print(f" Error ejecutando búsqueda multimedia: {e}")
            import traceback
            traceback.print_exc()
            raise ValueError(f"Error en búsqueda multimedia: {e}")
    
    def _load_metadata_for_multimedia(self, table_name: str, results: List[Tuple[str, float]], 
                                    fields: List[str]) -> Dict[str, Dict]:
        """Carga metadatos para los archivos encontrados - CORREGIDO"""
        try:
            import pandas as pd
            
            # Obtener ruta del CSV original
            csv_path = self.engine.table_file_paths.get(table_name)
            if not csv_path or not os.path.exists(csv_path):
                print(f" No se puede cargar metadatos para tabla {table_name}")
                return {}
            
            # Cargar CSV completo
            df = pd.read_csv(csv_path)
            
            # CORREGIDO: Usar la misma lógica de detección de columnas que en _create_multimedia_system
            path_column = None
            priority_columns = ['audio_path', 'image_path', 'file_path', 'path']
            
            for col in priority_columns:
                if col in df.columns:
                    path_column = col
                    break
            
            if not path_column:
                path_candidates = [col for col in df.columns if 'path' in col.lower() or 'file' in col.lower()]
                if path_candidates:
                    path_column = path_candidates[0]
            
            if not path_column:
                return {}
            
            # Crear mapeo de archivos a metadatos
            metadata = {}
            
            for _, row in df.iterrows():
                file_path = row[path_column]
                
                # Verificar si este archivo está en los resultados
                for result_path, _ in results:
                    if os.path.basename(file_path) == os.path.basename(result_path) or file_path == result_path:
                        metadata[result_path] = row.to_dict()
                        break
            
            return metadata
            
        except Exception as e:
            print(f" Error cargando metadatos: {e}")
            return {}
    
    def get_multimedia_table_info(self, table_name: str) -> Dict[str, Any]:
        """Obtiene información detallada de una tabla multimedia"""
        if table_name not in self.multimedia_engines:
            return {'error': f'Tabla multimedia {table_name} no encontrada'}
        
        engine = self.multimedia_engines[table_name]
        stats = engine.get_system_statistics()
        
        return {
            'table_name': table_name,
            'media_type': stats.get('media_type'),
            'feature_method': stats.get('feature_method'),
            'n_clusters': stats.get('n_clusters'),
            'features_extracted': stats.get('features_extracted'),
            'histograms_created': stats.get('histograms_created'),
            'is_built': stats.get('is_built'),
            'csv_path': self.engine.table_file_paths.get(table_name),
            'headers': self.engine.table_headers.get(table_name, [])
        }
    
    def list_multimedia_tables(self) -> List[Dict[str, Any]]:
        """Lista todas las tablas multimedia registradas"""
        tables = []
        for table_name in self.multimedia_engines.keys():
            tables.append(self.get_multimedia_table_info(table_name))
        return tables

    # ==================== MÉTODOS EXISTENTES (texto y tradicional) ====================
    
    def _parse_create_table(self, query: str) -> str:
        """
        Parsea CREATE TABLE con soporte específico para SPIMI
        """
        
        # Patrón para índices textuales con múltiples campos (SPIMI/InvertedIndex)
        textual_pattern = r'create\s+table\s+(\w+)\s+from\s+file\s+"([^"]+)"\s+using\s+index\s+(spimi|inverted|text)\s*\(\s*([^)]+)\s*\)'
        match = re.search(textual_pattern, query, re.IGNORECASE)
        
        if match:
            table_name = match.group(1)
            file_path = match.group(2)
            index_type = match.group(3).lower()
            fields_str = match.group(4)
            
            # Parsear múltiples campos textuales
            text_fields = self._parse_text_fields(fields_str)
            
            print(f" Creando tabla textual: {table_name}")
            print(f" Archivo: {file_path}")
            print(f" Tipo de índice: {index_type.upper()}")
            print(f" Campos textuales: {', '.join(text_fields)}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
            
            # Crear índice textual SPIMI
            return self._create_spimi_index(table_name, file_path, text_fields)
        
        # Patrón original para índices tradicionales
        traditional_pattern = r'create\s+table\s+(\w+)\s+from\s+file\s+"([^"]+)"\s+using\s+index\s+(\w+)\("?(\w+)"?\)'
        match = re.search(traditional_pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Sintaxis incorrecta para CREATE TABLE")
        
        table_name = match.group(1)
        file_path = match.group(2)
        index_type = match.group(3).lower()
        index_field_name = match.group(4)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        # Mapeo de índices tradicionales
        index_mapping = {
            'seq': 'sequential',
            'sequential': 'sequential',
            'isam': 'isam',
            'hash': 'hash',
            'btree': 'bplustree',
            'bplustree': 'bplustree',
            'rtree': 'rtree'
        }
        
        if index_type not in index_mapping:
            raise ValueError(f"Tipo de índice no soportado: {index_type}")
        
        mapped_index_type = index_mapping[index_type]
        index_field = self._get_column_index(file_path, index_field_name)
        
        result = self.engine.load_csv(
            table=table_name,
            path=file_path,
            tipo=mapped_index_type,
            index_field=index_field
        )
        
        return result

    def _create_spimi_index(self, table_name: str, file_path: str, text_fields: List[str]) -> str:
        """
        Crea un índice SPIMI específicamente para la tabla
        """
        try:
            # Verificar disponibilidad de módulos de texto
            try:
                from indices.spimi import SPIMIIndexBuilder
                SPIMI_AVAILABLE = True
            except ImportError as e:
                raise ValueError(f"SPIMI no disponible: {e}")
            
            print(f"Construyendo índice SPIMI para tabla '{table_name}'...")
            
            # Crear constructor SPIMI
            spimi_builder = SPIMIIndexBuilder(
                output_dir="indices",
                memory_limit_mb=100,  # Aumentar límite de memoria
                text_fields=text_fields,
                language='spanish'
            )
            
            # Construir índice usando load_csv
            start_time = time.time()
            index_path = spimi_builder.load_csv(file_path, text_fields, encoding='utf-8')
            construction_time = time.time() - start_time
            
            if not index_path:
                raise ValueError("Error construyendo índice SPIMI")
            
            print(f" Índice SPIMI construido en: {index_path}")
            print(f" Tiempo de construcción: {construction_time:.2f}s")
            
            # Registrar en el engine como índice textual
            self.engine.register_text_table(table_name, index_path, text_fields, file_path)
            
            # Mostrar estadísticas
            stats = spimi_builder.get_stats()
            if isinstance(stats, dict) and 'total_terms' in stats:
                print(f" Términos únicos: {stats['total_terms']:,}")
                print(f"Documentos: {stats.get('total_documents', 0):,}")
                print(f"Tamaño: {stats.get('index_size_mb', 0):.2f} MB")
            
            return f"Tabla '{table_name}' creada exitosamente con índice SPIMI. Campos indexados: {', '.join(text_fields)}"
                
        except Exception as e:
            print(f" Error creando índice SPIMI: {e}")
            import traceback
            traceback.print_exc()
            raise ValueError(f"Error creando tabla con índice SPIMI: {e}")

    def _parse_text_fields(self, fields_str: str) -> List[str]:
        """
        Parsea una lista de campos de texto separados por comas
        Ejemplo: "track_name", "track_artist", "lyrics" -> ['track_name', 'track_artist', 'lyrics']
        """
        fields = []
        current_field = ""
        in_quotes = False
        quote_char = None
        
        i = 0
        while i < len(fields_str):
            char = fields_str[i]
            
            if char in ['"', "'"] and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
            elif char == ',' and not in_quotes:
                field_name = current_field.strip().strip('"\'')
                if field_name:
                    fields.append(field_name)
                current_field = ""
                i += 1
                continue
            else:
                current_field += char
            
            i += 1
        
        # Agregar el último campo
        if current_field:
            field_name = current_field.strip().strip('"\'')
            if field_name:
                fields.append(field_name)
        
        return fields

    # ==================== MÉTODOS DE BÚSQUEDA TEXTUAL ====================

    def _parse_textual_search(self, query: str) -> List[str]:
        """
        Parsea consultas SQL con operador @@ para búsqueda textual
        Sintaxis: SELECT campos FROM tabla WHERE campo @@ 'consulta' LIMIT k;
        """
        # Limpiar query
        query_clean = query.strip().rstrip(';')
        
        # Patrones para capturar consultas textuales (incluyendo frases entre comillas)
        patterns = [
            # Patrón con frases entre comillas dobles
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+@@\s+"([^"]+)"\s+LIMIT\s+(\d+)',
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+@@\s+"([^"]+)"',
            # Patrón con comillas simples
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+@@\s+\'([^\']+)\'\s+LIMIT\s+(\d+)',
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+@@\s+\'([^\']+)\'',
        ]
        
        parsed_query = None
        
        for pattern in patterns:
            match = re.search(pattern, query_clean, re.IGNORECASE | re.DOTALL)
            if match:
                fields_str = match.group(1).strip()
                table = match.group(2).strip()
                search_field = match.group(3).strip()
                query_text = match.group(4).strip()
                limit = int(match.group(5)) if len(match.groups()) >= 5 and match.group(5) else 10
                
                # Procesar campos
                if fields_str.strip() == '*':
                    fields = ['*']
                else:
                    fields = [f.strip() for f in fields_str.split(',')]
                
                parsed_query = {
                    'fields': fields,
                    'table': table,
                    'search_field': search_field,
                    'query': query_text,
                    'limit': limit,
                    'original_sql': query
                }
                break
        
        if not parsed_query:
            raise ValueError("Sintaxis de consulta textual no válida. Use: SELECT campos FROM tabla WHERE campo @@ 'consulta' LIMIT k;")
        
        # Ejecutar búsqueda textual
        return self._execute_textual_query(parsed_query)
    
# parser_sql/parser.py - CORREGIR líneas 450-490 aproximadamente

# En parser.py, reemplazar el método _execute_textual_query:

    def _execute_textual_query(self, parsed: Dict[str, Any]) -> List[str]:
        """Ejecuta una consulta textual y retorna resultados formateados"""
        table_name = parsed['table']
        query_text = parsed['query']
        k = parsed['limit']
        fields = parsed['fields']
        
        print(f"\n EJECUTANDO BÚSQUEDA TEXTUAL")
        print("=" * 50)
        print(f" Consulta: '{query_text}'")
        print(f" Tabla: {table_name}")
        print(f" Top-K: {k}")
        print(f" Campos solicitados: {fields}")
        
        # Ejecutar búsqueda en el engine
        try:
            results = self.engine.textual_search(table_name, query_text, k)
            
            if not results:
                print(f" No se encontraron resultados para: '{query_text}'")
                return []
            
            # CORREGIDO: Ordenar resultados por score de mayor a menor
            sorted_results = sorted(results, key=lambda x: x[1], reverse=True)
            
            # Formatear resultados según campos solicitados
            formatted_results = []
            
            # CORREGIDO: Obtener headers originales de la tabla
            original_headers = self.engine.get_table_headers(table_name)
            
            for i, (doc, score) in enumerate(sorted_results, 1):
                if doc and isinstance(doc, dict):
                    # Mostrar información relevante
                    title = doc.get('track_name', doc.get('name', doc.get('title', 'Sin título')))
                    artist = doc.get('track_artist', doc.get('artists', doc.get('artist', 'Sin artista')))
                    
                    print(f"{i:2d}. [{score:.4f}] {title} - {artist}")
                    
                    # CORREGIDO: Construir CSV respetando exactamente el orden de campos
                    csv_values = []
                    
                    if '*' in fields:
                        # Para SELECT *, usar solo los campos que están en el documento
                        # y mantener el orden de los headers originales
                        field_order = []
                        for header in original_headers:
                            if header in doc:
                                field_order.append(header)
                        
                        # Si no hay campos coincidentes, usar campos básicos
                        if not field_order:
                            field_order = ['track_name', 'track_artist']
                    else:
                        # Para campos específicos, usar exactamente los solicitados
                        field_order = [f.strip() for f in fields]
                    
                    # CORREGIDO: Construir valores en el orden EXACTO
                    for field in field_order:
                        value = doc.get(field, '')
                        
                        # Escapar para CSV si contiene caracteres especiales
                        value_str = str(value)
                        if ',' in value_str or '"' in value_str or '\n' in value_str:
                            value_str = f'"{value_str.replace(chr(34), chr(34)*2)}"'
                        
                        csv_values.append(value_str)
                    
                    # CORREGIDO: Agregar score al final
                    csv_values.append(str(round(score, 4)))
                    
                    # Crear string CSV final
                    csv_record = ','.join(csv_values)
                    formatted_results.append(csv_record)
                    
                    # DEBUG: Mostrar mapeo para las primeras 3 filas
                    if i <= 3:
                        print(f"   DEBUG {i} - Campos: {field_order + ['similarity_score']}")
                        print(f"   DEBUG {i} - Valores: {csv_values}")
            
            print(f" Búsqueda completada: {len(formatted_results)} resultados")
            field_order = field_order if 'field_order' in locals() else []
            print(f" Orden final de campos: {field_order + ['similarity_score']}")
            
            return formatted_results
            
        except Exception as e:
            print(f" Error ejecutando búsqueda textual: {e}")
            import traceback
            traceback.print_exc()
            return []

    # ==================== MÉTODOS AUXILIARES EXISTENTES ====================
    
    def _get_column_index(self, file_path: str, column_name: str) -> int:
        """Obtiene el índice de una columna desde un archivo CSV"""
        try:
            import csv
            with open(file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                for i, header in enumerate(headers):
                    if header.strip().lower() == column_name.lower():
                        return i
                
                try:
                    return int(column_name)
                except ValueError:
                    pass
                
                raise ValueError(f"Columna '{column_name}' no encontrada en {headers}")
        
        except Exception as e:
            raise ValueError(f"Error leyendo archivo {file_path}: {e}")

    # ==================== RESTO DE MÉTODOS EXISTENTES (simplificados) ====================
    
    def _parse_select(self, query: str) -> List[str]:
        """Parsea SELECT básicos (solo SELECT * FROM tabla)"""
        # Si la consulta contiene operadores especiales, no debería llegar aquí
        if ' <-> ' in query or ' @@ ' in query:
            raise ValueError("Esta consulta debería ser manejada por otro parser")
        
        basic_pattern = r'select\s+\*\s+from\s+(\w+)'
        match = re.search(basic_pattern, query.strip(), re.IGNORECASE)
        if match:
            table_name = match.group(1)
            result = self.engine.scan(table_name)
            return result.split('\n') if result else []
        
        raise ValueError(f"Sintaxis SELECT no reconocida: {query}")
    
    def _parse_insert(self, query: str) -> str:
        """Parsea INSERT básicos"""
        pattern = r'insert\s+into\s+(\w+)\s+values\s*\(([^)]+)\)'
        match = re.search(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Sintaxis incorrecta para INSERT")
        
        table_name = match.group(1)
        values_str = match.group(2)
        values = self._parse_values(values_str)
        
        return self.engine.insert(table_name, values)
    
    def _parse_delete(self, query: str) -> List[str]:
        """Parsea DELETE básicos"""
        pattern = r'delete\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)'
        match = re.search(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Sintaxis incorrecta para DELETE")
        
        table_name = match.group(1)
        column_name = match.group(2)
        value = match.group(3).strip().strip('"\'')
        
        return self.engine.remove(table_name, value)

    def _parse_insert_generate(self, query: str) -> str:
        """Placeholder para generación de datos"""
        return "Generación de datos no implementada en esta versión"

    def _parse_values(self, values_str: str) -> List[str]:
        """Parsea una cadena de valores separados por comas"""
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        
        i = 0
        while i < len(values_str):
            char = values_str[i]
            
            if char in ['"', "'"] and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
            elif char == ',' and not in_quotes:
                values.append(current_value.strip().strip('"\''))
                current_value = ""
                i += 1
                continue
            else:
                current_value += char
            
            i += 1
        
        if current_value:
            values.append(current_value.strip().strip('"\''))
        
        return values