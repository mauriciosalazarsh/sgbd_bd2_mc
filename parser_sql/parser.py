# parser_sql/parser.py - VERSIÓN CORREGIDA PARA SPIMI

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
        
    def register_text_index(self, table_name: str, text_index):
        """Registra un índice textual para una tabla"""
        self.text_indices[table_name] = text_index
        print(f"📝 Índice textual registrado para tabla: {table_name}")
        
    def parse_and_execute(self, query: str) -> Any:
        """
        Parsea y ejecuta una consulta SQL (incluye generación de datos y consultas textuales)
        """
        query = query.strip().rstrip(';')
        query_lower = query.lower()
        
        # NUEVO: Verificar si es consulta textual con operador @@
        if ' @@ ' in query:
            return self._parse_textual_search(query)
        elif query_lower.startswith('create table'):
            return self._parse_create_table(query)
        elif query_lower.startswith('select'):
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
            
            print(f"🔧 Creando tabla textual: {table_name}")
            print(f"📁 Archivo: {file_path}")
            print(f"🔧 Tipo de índice: {index_type.upper()}")
            print(f"📋 Campos textuales: {', '.join(text_fields)}")
            
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
            
            print(f"🔨 Construyendo índice SPIMI para tabla '{table_name}'...")
            
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
            
            print(f"💾 Índice SPIMI construido en: {index_path}")
            print(f"⏱️ Tiempo de construcción: {construction_time:.2f}s")
            
            # Registrar en el engine como índice textual
            self.engine.register_text_table(table_name, index_path, text_fields, file_path)
            
            # Mostrar estadísticas
            stats = spimi_builder.get_stats()
            if isinstance(stats, dict) and 'total_terms' in stats:
                print(f"📊 Términos únicos: {stats['total_terms']:,}")
                print(f"📄 Documentos: {stats.get('total_documents', 0):,}")
                print(f"💽 Tamaño: {stats.get('index_size_mb', 0):.2f} MB")
            
            return f"Tabla '{table_name}' creada exitosamente con índice SPIMI. Campos indexados: {', '.join(text_fields)}"
                
        except Exception as e:
            print(f"❌ Error creando índice SPIMI: {e}")
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
    
    def _execute_textual_query(self, parsed: Dict[str, Any]) -> List[str]:
        """Ejecuta una consulta textual y retorna resultados formateados"""
        table_name = parsed['table']
        query_text = parsed['query']
        k = parsed['limit']
        fields = parsed['fields']
        
        print(f"\n🔍 EJECUTANDO BÚSQUEDA TEXTUAL")
        print("=" * 50)
        print(f"📝 Consulta: '{query_text}'")
        print(f"🎯 Tabla: {table_name}")
        print(f"📊 Top-K: {k}")
        print(f"📋 Campos solicitados: {fields}")
        
        # Ejecutar búsqueda en el engine
        try:
            results = self.engine.textual_search(table_name, query_text, k)
            
            if not results:
                print(f"⚠️ No se encontraron resultados para: '{query_text}'")
                return []
            
            # Formatear resultados según campos solicitados
            formatted_results = []
            
            for i, (doc, score) in enumerate(results, 1):
                if doc and isinstance(doc, dict):
                    # Mostrar información relevante
                    title = doc.get('track_name', doc.get('name', doc.get('title', 'Sin título')))
                    artist = doc.get('track_artist', doc.get('artists', doc.get('artist', 'Sin artista')))
                    
                    print(f"{i:2d}. [{score:.4f}] {title} - {artist}")
                    
                    # Crear registro CSV con campos solicitados
                    csv_values = []
                    
                    if '*' in fields:
                        # Usar todos los campos disponibles
                        field_order = list(doc.keys())
                    else:
                        # Usar solo campos específicos
                        field_order = fields
                    
                    for field in field_order:
                        if field == '_score':
                            continue
                        value = doc.get(field, '')
                        # Escapar para CSV
                        if ',' in str(value) or '"' in str(value) or '\n' in str(value):
                            value = f'"{str(value).replace('"', '""')}"'
                        csv_values.append(str(value))
                    
                    # Agregar score al final
                    csv_values.append(str(round(score, 4)))
                    
                    # Crear string CSV
                    csv_record = ','.join(csv_values)
                    formatted_results.append(csv_record)
            
            print(f"✅ Búsqueda completada: {len(formatted_results)} resultados")
            return formatted_results
            
        except Exception as e:
            print(f"❌ Error ejecutando búsqueda textual: {e}")
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
        """Parsea SELECT básicos"""
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