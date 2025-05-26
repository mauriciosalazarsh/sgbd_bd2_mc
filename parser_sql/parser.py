import re
import os
from typing import List, Dict, Any, Optional, Tuple

class SQLParser:
    def __init__(self, engine):
        self.engine = engine
        
    def parse_and_execute(self, query: str) -> Any:
        """
        Parsea y ejecuta una consulta SQL
        """
        query = query.strip().rstrip(';')
        
        # Determinar el tipo de consulta
        query_lower = query.lower()
        
        if query_lower.startswith('create table'):
            return self._parse_create_table(query)
        elif query_lower.startswith('select'):
            return self._parse_select(query)
        elif query_lower.startswith('insert'):
            return self._parse_insert(query)
        elif query_lower.startswith('delete'):
            return self._parse_delete(query)
        else:
            raise ValueError(f"Tipo de consulta no soportado: {query}")
    
    def _parse_create_table(self, query: str) -> str:
        """
        Parsea: create table Restaurantes from file "C:/restaurantes.csv" using index isam("id")
        """
        # Patrón regex para capturar los componentes
        pattern = r'create\s+table\s+(\w+)\s+from\s+file\s+"([^"]+)"\s+using\s+index\s+(\w+)\("?(\w+)"?\)'
        match = re.search(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Sintaxis incorrecta para CREATE TABLE")
        
        table_name = match.group(1)
        file_path = match.group(2)
        index_type = match.group(3).lower()
        index_field_name = match.group(4)
        
        # Validar archivo
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        # Mapear tipos de índice
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
        
        # Obtener el índice de la columna
        index_field = self._get_column_index(file_path, index_field_name)
        
        # Ejecutar la creación
        result = self.engine.load_csv(
            table=table_name,
            path=file_path,
            tipo=mapped_index_type,
            index_field=index_field
        )
        
        return result
    
   # REEMPLAZA EL MÉTODO _parse_select en parser_sql/parser.py

    def _parse_select(self, query: str) -> List[str]:
        """
        Parsea diferentes tipos de SELECT con prioridad correcta
        """
        # SELECT básico sin WHERE
        basic_pattern = r'select\s+\*\s+from\s+(\w+)$'
        match = re.search(basic_pattern, query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            result = self.engine.scan(table_name)
            return result.split('\n') if result else []
        
        # SELECT con IN (consultas espaciales) - PRIORIDAD ALTA
        # Patrón mejorado que maneja espacios y diferentes formatos
        in_pattern = r'select\s+\*\s+from\s+(\w+)\s+where\s+(\w+)\s+in\s*\(\s*"([^"]+)"\s*,\s*([^)]+)\s*\)'
        match = re.search(in_pattern, query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            column_name = match.group(2)
            point = match.group(3).strip()
            param = match.group(4).strip().strip('"\'')
            
            # Verificar que es una tabla R-Tree
            if table_name in self.engine.tables:
                idx = self.engine.tables[table_name]
                from indices.rtree import MultidimensionalRTree
                if isinstance(idx, MultidimensionalRTree):
                    return self.engine.range_search(table_name, point, param)
            
            return self.engine.range_search(table_name, point, param)
        
        # SELECT con WHERE simple (equality)
        equality_pattern = r'select\s+\*\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)'
        match = re.search(equality_pattern, query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            column_name = match.group(2)
            value = match.group(3).strip().strip('"\'')
            column_index = self._get_column_index_from_table(table_name, column_name)
            return self.engine.search(table_name, value, column_index)
        
        # SELECT con BETWEEN
        between_pattern = r'select\s+\*\s+from\s+(\w+)\s+where\s+(\w+)\s+between\s+(.+)\s+and\s+(.+)'
        match = re.search(between_pattern, query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            column_name = match.group(2)
            begin_key = match.group(3).strip().strip('"\'')
            end_key = match.group(4).strip().strip('"\'')
            return self.engine.range_search(table_name, begin_key, end_key)
        
        raise ValueError(f"Sintaxis SELECT no reconocida: {query}")
    
    def _parse_insert(self, query: str) -> str:
        """
        Parsea: insert into Restaurantes values (val1, val2, val3, ...)
        """
        pattern = r'insert\s+into\s+(\w+)\s+values\s*\(([^)]+)\)'
        match = re.search(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Sintaxis incorrecta para INSERT")
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        # Parsear valores
        values = self._parse_values(values_str)
        
        return self.engine.insert(table_name, values)
    
    def _parse_delete(self, query: str) -> List[str]:
        """
        Parsea: delete from Restaurantes where id = x
        """
        pattern = r'delete\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)'
        match = re.search(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Sintaxis incorrecta para DELETE")
        
        table_name = match.group(1)
        column_name = match.group(2)
        value = match.group(3).strip().strip('"\'')
        
        return self.engine.remove(table_name, value)
    
    def _get_column_index(self, file_path: str, column_name: str) -> int:
        """
        Obtiene el índice de una columna desde un archivo CSV
        """
        try:
            import csv
            with open(file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Buscar el índice por nombre
                for i, header in enumerate(headers):
                    if header.strip().lower() == column_name.lower():
                        return i
                
                # Si no encuentra por nombre, intentar convertir a número
                try:
                    return int(column_name)
                except ValueError:
                    pass
                
                raise ValueError(f"Columna '{column_name}' no encontrada en {headers}")
        
        except Exception as e:
            raise ValueError(f"Error leyendo archivo {file_path}: {e}")
    
    def _get_column_index_from_table(self, table_name: str, column_name: str) -> int:
        """
        Obtiene el índice de una columna desde una tabla ya cargada
        """
        if table_name not in self.engine.tables:
            raise ValueError(f"Tabla '{table_name}' no encontrada")
        
        # Obtener una muestra para ver las columnas
        try:
            sample = self.engine.scan(table_name)
            if sample:
                first_row = sample.split('\n')[0]
                headers = [col.strip() for col in first_row.split('|')]
                
                # Buscar por nombre
                for i, header in enumerate(headers):
                    if header.lower() == column_name.lower():
                        return i
                
                # Si no encuentra por nombre, intentar convertir a número
                try:
                    index = int(column_name)
                    if 0 <= index < len(headers):
                        return index
                except ValueError:
                    pass
        except:
            pass
        
        # Fallback: usar el campo indexado de la tabla
        index = self.engine.tables[table_name]
        if hasattr(index, 'field_index'):
            return index.field_index
        
        # Último recurso: asumir columna 0
        return 0
    
    def _parse_values(self, values_str: str) -> List[str]:
        """
        Parsea una cadena de valores separados por comas
        """
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
        
        # Agregar el último valor
        if current_value:
            values.append(current_value.strip().strip('"\''))
        
        return values
    
    def validate_syntax(self, query: str) -> bool:
        """
        Valida la sintaxis básica de una consulta
        """
        try:
            query = query.strip().lower()
            
            # Validaciones básicas
            if not query:
                return False
            
            # Verificar que termine en punto y coma o no (ambos son válidos)
            if query.endswith(';'):
                query = query[:-1]
            
            # Verificar palabras clave válidas
            valid_starts = ['create', 'select', 'insert', 'delete']
            return any(query.startswith(start) for start in valid_starts)
        
        except Exception:
            return False
    
    def get_query_type(self, query: str) -> str:
        """
        Determina el tipo de consulta
        """
        query = query.strip().lower()
        
        if query.startswith('create'):
            return 'CREATE'
        elif query.startswith('select'):
            return 'SELECT'
        elif query.startswith('insert'):
            return 'INSERT'
        elif query.startswith('delete'):
            return 'DELETE'
        else:
            return 'UNKNOWN'
    
    def suggest_correction(self, query: str) -> str:
        """
        Sugiere correcciones para consultas con errores comunes
        """
        corrections = []
        query_lower = query.lower()
        
        # Correcciones comunes
        if 'select' in query_lower and 'form' in query_lower:
            corrections.append("¿Quisiste decir 'FROM' en lugar de 'FORM'?")
        
        if 'insert' in query_lower and 'value' in query_lower and 'values' not in query_lower:
            corrections.append("¿Quisiste decir 'VALUES' en lugar de 'VALUE'?")
        
        if 'create' in query_lower and 'file' in query_lower and '"' not in query:
            corrections.append("El path del archivo debe estar entre comillas dobles")
        
        return " | ".join(corrections) if corrections else "No se encontraron sugerencias"