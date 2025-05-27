# parser_sql/parser.py - VERSIÓN FINAL COMPLETA CON TODAS LAS CORRECCIONES

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
        
    def parse_and_execute(self, query: str) -> Any:
        """
        Parsea y ejecuta una consulta SQL (incluye generación de datos)
        """
        query = query.strip().rstrip(';')
        
        # Determinar el tipo de consulta
        query_lower = query.lower()
        
        if query_lower.startswith('create table'):
            return self._parse_create_table(query)
        elif query_lower.startswith('select'):
            return self._parse_select(query)
        elif query_lower.startswith('insert'):
            # NUEVO: Detectar si es INSERT con generación de datos
            if 'generate_series' in query_lower or 'generate_data' in query_lower:
                return self._parse_insert_generate(query)
            else:
                return self._parse_insert(query)
        elif query_lower.startswith('delete'):
            return self._parse_delete(query)
        else:
            raise ValueError(f"Tipo de consulta no soportado: {query}")

    def _parse_insert_generate(self, query: str) -> str:
        """
        Parsea INSERT con generación de datos masivos
        """
        # Patrón para INSERT ... SELECT ... FROM GENERATE_SERIES(start, end)
        pattern = r'insert\s+into\s+(\w+)\s+select\s+(.+?)\s+from\s+generate_series\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)'
        match = re.search(pattern, query, re.IGNORECASE | re.DOTALL)
        
        if match:
            table_name = match.group(1)
            select_clause = match.group(2)
            start = int(match.group(3))
            end = int(match.group(4))
            count = end - start + 1
            
            return self._generate_from_select(table_name, select_clause, count, start)
        
        # Patrón para INSERT INTO table GENERATE_DATA(count)
        pattern2 = r'insert\s+into\s+(\w+)\s+generate_data\s*\(\s*(\d+)\s*\)'
        match = re.search(pattern2, query, re.IGNORECASE)
        
        if match:
            table_name = match.group(1)
            count = int(match.group(2))
            return self._generate_simple_data(table_name, count)
        
        raise ValueError("Sintaxis de generación de datos no reconocida")

    def _generate_simple_data(self, table_name: str, count: int) -> str:
        """
        Genera datos simples basados en las columnas existentes - CORREGIDO para ISAM
        """
        if table_name not in self.engine.tables:
            raise ValueError(f"Tabla '{table_name}' no encontrada")
        
        headers = self.engine.get_table_headers(table_name)
        
        # CORRECCIÓN: Si no hay headers, usar esquema por defecto
        if not headers:
            print(f"⚠️ No se encontraron headers para '{table_name}', usando esquema por defecto")
            # Para ISAM, intentar obtener el esquema desde la tabla
            idx = self.engine.tables[table_name]
            if hasattr(idx, 'schema') and idx.schema:
                headers = [f"col{i}" for i in range(len(idx.schema))]
            else:
                # Esquema por defecto con 4 columnas típicas
                headers = ["id", "name", "age", "category"]
        
        print(f"🚀 Generando {count:,} registros para {table_name}...")
        print(f"📋 Esquema: {len(headers)} columnas: {headers}")
        
        start_time = time.time()
        inserted_count = 0
        batch_size = 1000  # Batch más grande para mejor rendimiento
        
        try:
            for i in range(count):
                # Generar registro con ID incremental a partir de 1000
                record = self._generate_synthetic_record(headers, 1000 + i)
                
                # Debug para los primeros 3 registros
                if i < 3:
                    print(f"🔍 Registro {i+1}: {len(record)} columnas: {record}")
                
                # Verificar que el registro tenga el número correcto de columnas
                if len(record) != len(headers):
                    # Ajustar automáticamente
                    if len(record) > len(headers):
                        record = record[:len(headers)]
                    else:
                        while len(record) < len(headers):
                            record.append(f"default_{len(record)}")
                    
                    if i < 3:  # Solo mostrar para los primeros registros
                        print(f"⚠️ Ajustado a {len(headers)} columnas: {record}")
                
                # Insertar en la tabla (el engine maneja la conversión para ISAM)
                self.engine.insert(table_name, record)
                inserted_count += 1
                
                # Progress cada batch
                if (i + 1) % batch_size == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    print(f"✅ {i + 1:,}/{count:,} registros ({rate:.0f} reg/seg)")
            
            end_time = time.time()
            total_time = end_time - start_time
            rate = inserted_count / total_time if total_time > 0 else 0
            
            return f"✅ Inserción completada: {inserted_count:,} registros en {total_time:.2f}s ({rate:.0f} reg/seg)"
            
        except Exception as e:
            print(f"❌ Error detallado: {e}")
            print(f"📊 Estado: {inserted_count} registros insertados de {count}")
            if 'record' in locals():
                print(f"🔍 Último registro problemático: {len(record)} columnas: {record}")
            return f"❌ Error: {e}. Insertados {inserted_count:,} registros"

    def _generate_synthetic_record(self, headers: List[str], index: int) -> List[str]:
        """
        Genera un registro sintético basado en los nombres de las columnas - MEJORADO
        """
        record = []
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            
            # ID fields (siempre usar el índice proporcionado)
            if any(keyword in header_lower for keyword in ['id', 'index', 'number', 'col0']):
                record.append(str(index))
            
            # Name fields
            elif any(keyword in header_lower for keyword in ['name', 'nombre', 'student', 'col1']):
                names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry', 'Isabel', 'Jack']
                record.append(f"{random.choice(names)}_{index}")
            
            # Age fields
            elif any(keyword in header_lower for keyword in ['age', 'edad', 'col2']):
                record.append(str(random.randint(18, 65)))
            
            # Category/Group fields
            elif any(keyword in header_lower for keyword in ['category', 'group', 'tipo', 'col3']):
                categories = ['A', 'B', 'C', 'D']
                record.append(random.choice(categories))
            
            # Gender fields
            elif any(keyword in header_lower for keyword in ['gender', 'sex', 'sexo']):
                record.append(random.choice(['male', 'female']))
            
            # Score/Grade fields
            elif any(keyword in header_lower for keyword in ['score', 'grade', 'nota', 'calificacion']):
                record.append(str(random.randint(0, 100)))
            
            # Education fields
            elif any(keyword in header_lower for keyword in ['education', 'school', 'parental']):
                education_levels = ['high school', 'bachelor', 'master', 'phd']
                record.append(random.choice(education_levels))
            
            # Lunch fields
            elif any(keyword in header_lower for keyword in ['lunch']):
                record.append(random.choice(['standard', 'free']))
            
            # Test preparation
            elif any(keyword in header_lower for keyword in ['test', 'preparation', 'course']):
                record.append(random.choice(['none', 'completed']))
            
            # Location fields (lat/lon)
            elif any(keyword in header_lower for keyword in ['lat', 'latitude']):
                record.append(str(round(random.uniform(-90, 90), 6)))
            
            elif any(keyword in header_lower for keyword in ['lon', 'lng', 'longitude']):
                record.append(str(round(random.uniform(-180, 180), 6)))
            
            # Default: string con índice
            else:
                record.append(f"data_{index}_{i}")
        
        return record

    def _generate_from_select(self, table_name: str, select_clause: str, count: int, start_id: int = 1) -> str:
        """
        Genera datos basados en una cláusula SELECT personalizada
        """
        if table_name not in self.engine.tables:
            raise ValueError(f"Tabla '{table_name}' no encontrada")
        
        # Obtener información del esquema de la tabla
        headers = self.engine.get_table_headers(table_name)
        expected_columns = len(headers) if headers else 0
        
        print(f"🚀 Generando {count:,} registros para {table_name}...")
        print(f"📋 Esquema esperado: {expected_columns} columnas: {headers}")
        
        # Guardar referencia para ajuste automático
        self._current_table = table_name
        
        start_time = time.time()
        inserted_count = 0
        batch_size = 500  # Reducir batch size para mejor debugging
        
        try:
            for i in range(count):
                current_row = start_id + i
                
                # Evaluar SELECT clause para este row_number
                record = self._evaluate_select_clause(select_clause, current_row)
                
                # Verificación adicional de seguridad
                if expected_columns > 0 and len(record) != expected_columns:
                    # Ajustar automáticamente
                    if len(record) > expected_columns:
                        record = record[:expected_columns]
                    else:
                        while len(record) < expected_columns:
                            record.append("")
                
                # Debug para los primeros registros
                if i < 3:
                    print(f"🔍 Registro {i+1}: {len(record)} columnas: {record}")
                
                # Insertar en la tabla
                self.engine.insert(table_name, record)
                inserted_count += 1
                
                # Progress cada batch
                if (i + 1) % batch_size == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    print(f"✅ {i + 1:,}/{count:,} registros ({rate:.0f} reg/seg)")
            
            end_time = time.time()
            total_time = end_time - start_time
            rate = inserted_count / total_time if total_time > 0 else 0
            
            return f"✅ Inserción completada: {inserted_count:,} registros en {total_time:.2f}s ({rate:.0f} reg/seg)"
            
        except Exception as e:
            print(f"❌ Error detallado: {e}")
            print(f"📊 Estado: {inserted_count} registros insertados de {count}")
            if 'record' in locals():
                print(f"🔍 Último registro problemático: {len(record)} columnas: {record}")
            return f"❌ Error: {e}. Insertados {inserted_count:,} registros"
        finally:
            # Limpiar referencia
            if hasattr(self, '_current_table'):
                delattr(self, '_current_table')

    def _evaluate_select_clause(self, select_clause: str, row_number: int) -> List[str]:
        """
        Evalúa una cláusula SELECT y genera valores ajustados al esquema de la tabla
        """
        table_name = None
        # Intentar obtener el nombre de la tabla del contexto
        if hasattr(self, '_current_table'):
            table_name = self._current_table
        
        # Obtener headers esperados
        expected_headers = []
        if table_name and table_name in self.engine.tables:
            expected_headers = self.engine.get_table_headers(table_name)
        
        # Separar las columnas por comas (respetando paréntesis)
        columns = self._split_select_columns(select_clause)
        record = []
        
        for col in columns:
            col = col.strip()
            value = self._evaluate_expression(col, row_number)
            record.append(str(value))
        
        # Ajustar al número de columnas esperadas
        if expected_headers:
            expected_count = len(expected_headers)
            current_count = len(record)
            
            if current_count > expected_count:
                # Truncar si hay demasiadas columnas
                record = record[:expected_count]
                print(f"⚠️ Truncando de {current_count} a {expected_count} columnas")
            elif current_count < expected_count:
                # Rellenar con valores por defecto
                while len(record) < expected_count:
                    record.append(f"default_{len(record)}")
                print(f"⚠️ Rellenando de {current_count} a {expected_count} columnas")
        
        return record

    def _split_select_columns(self, select_clause: str) -> List[str]:
        """
        Divide las columnas del SELECT respetando paréntesis y CASE statements
        """
        columns = []
        current = ""
        paren_count = 0
        in_case = False
        
        i = 0
        while i < len(select_clause):
            char = select_clause[i]
            
            # Detectar CASE statements
            if select_clause[i:i+4].upper() == 'CASE':
                in_case = True
            elif select_clause[i:i+3].upper() == 'END':
                in_case = False
                current += select_clause[i:i+3]
                i += 2
                i += 1
                continue
            
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0 and not in_case:
                columns.append(current.strip())
                current = ""
                i += 1
                continue
            
            current += char
            i += 1
        
        if current.strip():
            columns.append(current.strip())
        
        return columns

    def _evaluate_expression(self, expression: str, row_number: int) -> Any:
        """
        Evalúa una expresión SQL y devuelve un valor
        """
        expr = expression.strip()
        
        # Manejar alias (AS)
        as_match = re.search(r'(.+?)\s+as\s+\w+', expr, re.IGNORECASE)
        if as_match:
            expr = as_match.group(1).strip()
        
        # MD5 hash
        if 'md5(' in expr.lower():
            if 'random()' in expr:
                random_text = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
                return hashlib.md5(random_text.encode()).hexdigest()[:8]
            else:
                return hashlib.md5(str(row_number).encode()).hexdigest()[:8]
        
        # CONCAT function
        if expr.lower().startswith('concat('):
            # concat('user_', row_number)
            concat_match = re.search(r"concat\s*\(\s*'([^']+)'\s*,\s*(\w+)\s*\)", expr, re.IGNORECASE)
            if concat_match:
                prefix = concat_match.group(1)
                if concat_match.group(2).lower() == 'row_number':
                    return f"{prefix}{row_number}"
                else:
                    return f"{prefix}{row_number}"
            return f"concat_{row_number}"
        
        # CASE statements
        if expr.upper().startswith('CASE'):
            return self._evaluate_case_statement(expr, row_number)
        
        # Arithmetic with row_number
        if 'row_number' in expr.lower():
            # (1000 + row_number)
            if '+' in expr:
                add_match = re.search(r'\((\d+)\s*\+\s*row_number\)', expr, re.IGNORECASE)
                if add_match:
                    base = int(add_match.group(1))
                    return base + row_number
            
            # (50 + (row_number % 50))
            mod_match = re.search(r'\((\d+)\s*\+\s*\(row_number\s*%\s*(\d+)\)\)', expr, re.IGNORECASE)
            if mod_match:
                base = int(mod_match.group(1))
                mod = int(mod_match.group(2))
                return base + (row_number % mod)
            
            return row_number
        
        # Random expressions
        if 'random()' in expr:
            # (random() * 40 + 60)::int
            if '::int' in expr:
                mult_match = re.search(r'random\(\)\s*\*\s*([\d.]+)', expr)
                add_match = re.search(r'\+\s*([\d.]+)', expr)
                
                mult = float(mult_match.group(1)) if mult_match else 1.0
                add = float(add_match.group(1)) if add_match else 0.0
                
                return int(random.random() * mult + add)
            
            # round(random() * 100, 2)
            elif 'round(' in expr:
                round_match = re.search(r'round\(random\(\)\s*\*\s*([\d.]+)\s*,\s*(\d+)\)', expr)
                if round_match:
                    mult = float(round_match.group(1))
                    decimals = int(round_match.group(2))
                    return round(random.random() * mult, decimals)
                else:
                    return round(random.random() * 100, 2)
            
            # random() < 0.5 (boolean)
            elif '<' in expr:
                threshold_match = re.search(r'random\(\)\s*<\s*([\d.]+)', expr)
                if threshold_match:
                    threshold = float(threshold_match.group(1))
                    return random.random() < threshold
                else:
                    return random.random() < 0.5
            
            return random.random()
        
        # Modulo operations
        mod_match = re.search(r'\(row_number\s*%\s*(\d+)\)', expr, re.IGNORECASE)
        if mod_match:
            mod_val = int(mod_match.group(1))
            return row_number % mod_val
        
        # Comparison with modulo
        eq_match = re.search(r'\(row_number\s*%\s*(\d+)\)\s*=\s*(\d+)', expr, re.IGNORECASE)
        if eq_match:
            mod_val = int(eq_match.group(1))
            eq_val = int(eq_match.group(2))
            return (row_number % mod_val) == eq_val
        
        # Literals
        if expr.startswith("'") and expr.endswith("'"):
            return expr[1:-1]
        
        # Numbers
        try:
            if '.' in expr:
                return float(expr)
            else:
                return int(expr)
        except ValueError:
            pass
        
        # Fallback
        return f"value_{row_number}"

    def _evaluate_case_statement(self, case_expr: str, row_number: int) -> str:
        """
        Evalúa un CASE statement SQL
        """
        case_expr = case_expr.strip()
        
        # CASE WHEN random() < 0.5 THEN 'male' ELSE 'female' END
        simple_random_match = re.search(
            r"case\s+when\s+random\(\)\s*<\s*([\d.]+)\s+then\s+'([^']+)'\s+else\s+'([^']+)'\s+end",
            case_expr, re.IGNORECASE
        )
        if simple_random_match:
            threshold = float(simple_random_match.group(1))
            true_val = simple_random_match.group(2)
            false_val = simple_random_match.group(3)
            return true_val if random.random() < threshold else false_val
        
        # CASE WHEN (row_number % 2) = 0 THEN 'male' ELSE 'female' END
        mod_match = re.search(
            r"case\s+when\s+\(row_number\s*%\s*(\d+)\)\s*=\s*(\d+)\s+then\s+'([^']+)'\s+else\s+'([^']+)'\s+end",
            case_expr, re.IGNORECASE
        )
        if mod_match:
            mod_val = int(mod_match.group(1))
            eq_val = int(mod_match.group(2))
            true_val = mod_match.group(3)
            false_val = mod_match.group(4)
            return true_val if (row_number % mod_val) == eq_val else false_val
        
        # Fallback genérico
        return f"case_result_{row_number % 4}"

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

    def _parse_select(self, query: str) -> List[str]:
        """
        Parsea diferentes tipos de SELECT con DEBUG MEJORADO
        """
        print(f"\n🔍 PARSEANDO SELECT: {query}")
        
        # SELECT básico sin WHERE
        basic_pattern = r'select\s+\*\s+from\s+(\w+)$'
        match = re.search(basic_pattern, query.strip(), re.IGNORECASE)
        if match:
            table_name = match.group(1)
            print(f"✅ SELECT básico detectado para tabla: {table_name}")
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
            
            return self.engine.range_search(table_name, point, param)
        
        # SELECT con WHERE simple (equality) - CON DEBUG COMPLETO
        equality_pattern = r'select\s+\*\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)'
        match = re.search(equality_pattern, query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            column_name = match.group(2)
            value = match.group(3).strip().strip('"\'')
            
            print(f"✅ SELECT con WHERE detectado:")
            print(f"   - Tabla: {table_name}")
            print(f"   - Columna: {column_name}")
            print(f"   - Valor: {value}")
            
            try:
                # Obtener índice de columna CON DEBUG
                column_index = self._get_column_index_from_table(table_name, column_name)
                
                # Ejecutar búsqueda CON DEBUG
                print(f"\n🔄 EJECUTANDO BÚSQUEDA:")
                print(f"   - engine.search('{table_name}', '{value}', {column_index})")
                
                result = self.engine.search(table_name, value, column_index)
                
                print(f"🎯 RESULTADO: {len(result)} registros encontrados")
                if len(result) > 0:
                    print(f"   - Primer resultado: {result[0][:100]}...")
                
                return result
                
            except Exception as e:
                print(f"❌ ERROR en búsqueda: {e}")
                import traceback
                traceback.print_exc()
                return []
        
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
    
# En parser_sql/parser.py - REEMPLAZAR el método _parse_delete con esta versión corregida:

    def _parse_delete(self, query: str) -> List[str]:
        """
        Parsea: delete from Restaurantes where columna = valor
        CORREGIDO: Ahora maneja correctamente búsquedas en cualquier columna
        """
        pattern = r'delete\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)'
        match = re.search(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Sintaxis incorrecta para DELETE")
        
        table_name = match.group(1)
        column_name = match.group(2)
        value = match.group(3).strip().strip('"\'')
        
        print(f"\n🗑️ DELETE PARSER:")
        print(f"   - Tabla: {table_name}")
        print(f"   - Columna: {column_name}")
        print(f"   - Valor: {value}")
        
        # CORREGIDO: Primero buscar los registros que coinciden, luego eliminarlos
        try:
            # 1. Obtener el índice de la columna
            column_index = self._get_column_index_from_table(table_name, column_name)
            print(f"   - Índice de columna: {column_index}")
            
            # 2. Buscar registros que coinciden
            matching_records = self.engine.search(table_name, value, column_index)
            print(f"   - Registros encontrados: {len(matching_records)}")
            
            if not matching_records:
                print("   ⚠️ No se encontraron registros para eliminar")
                return []
            
            # 3. Para el DELETE, necesitamos eliminar por la clave del índice principal
            idx = self.engine.tables[table_name]
            field_index = getattr(idx, 'field_index', 0)
            
            deleted_records = []
            
            # 4. Para cada registro encontrado, extraer la clave del índice y eliminar
            for record in matching_records:
                try:
                    # Parsear el registro CSV para obtener la clave del índice principal
                    import csv
                    import io
                    reader = csv.reader(io.StringIO(record.strip()))
                    cols = next(reader, [])
                    
                    if field_index < len(cols):
                        index_key = cols[field_index].strip()
                        print(f"   🔑 Eliminando por clave del índice: '{index_key}' (columna {field_index})")
                        
                        # Eliminar usando la clave del índice principal
                        removed = self.engine.remove(table_name, index_key)
                        deleted_records.extend(removed)
                    else:
                        print(f"   ❌ Error: Índice {field_index} fuera de rango para registro")
                        
                except Exception as e:
                    print(f"   ❌ Error procesando registro para DELETE: {e}")
                    continue
            
            print(f"   ✅ Total eliminados: {len(deleted_records)} registros")
            return deleted_records
            
        except Exception as e:
            print(f"   ❌ Error en DELETE: {e}")
            # FALLBACK: Si falla la búsqueda avanzada, intentar eliminación directa
            # (esto solo funciona si la columna WHERE es la misma que el índice)
            print(f"   🔄 Intentando eliminación directa...")
            try:
                return self.engine.remove(table_name, value)
            except Exception as e2:
                print(f"   ❌ Error en eliminación directa: {e2}")
                return []
    
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
        Obtiene el índice de una columna desde una tabla ya cargada - VERSIÓN ULTRA ROBUSTA
        """
        if table_name not in self.engine.tables:
            raise ValueError(f"Tabla '{table_name}' no encontrada")
        
        headers = self.engine.get_table_headers(table_name)
        
        if not headers:
            print(f"❌ No hay headers para tabla '{table_name}'")
            return 0
        
        # Normalizar nombre de columna buscada
        search_name = column_name.lower().strip()
        
        print(f"\n🔍 BUSCANDO COLUMNA:")
        print(f"   - Tabla: {table_name}")
        print(f"   - Columna buscada: '{column_name}' → normalizada: '{search_name}'")
        print(f"   - Headers disponibles ({len(headers)}): {headers}")
        
        # ESTRATEGIA 1: Búsqueda exacta (case insensitive)
        for i, header in enumerate(headers):
            header_normalized = header.strip().lower()
            if header_normalized == search_name:
                print(f"✅ MATCH EXACTO: '{header}' en posición {i}")
                return i
        
        # ESTRATEGIA 2: Normalización de espacios/guiones
        search_normalized = search_name.replace('_', ' ').replace('-', ' ').replace('.', ' ')
        print(f"   - Búsqueda normalizada (espacios): '{search_normalized}'")
        
        for i, header in enumerate(headers):
            header_normalized = header.strip().lower().replace('_', ' ').replace('-', ' ').replace('.', ' ')
            if header_normalized == search_normalized:
                print(f"✅ MATCH NORMALIZADO: '{header}' → '{header_normalized}' en posición {i}")
                return i
        
        # ESTRATEGIA 3: Buscar palabras clave
        search_words = search_normalized.split()
        print(f"   - Palabras clave: {search_words}")
        
        for i, header in enumerate(headers):
            header_words = header.strip().lower().replace('_', ' ').replace('-', ' ').split()
            
            # Verificar si todas las palabras de búsqueda están en el header
            if all(word in header_words for word in search_words):
                print(f"✅ MATCH POR PALABRAS: '{header}' contiene {search_words} en posición {i}")
                return i
            
            # O si alguna palabra clave importante coincide
            for search_word in search_words:
                if len(search_word) > 3 and search_word in ' '.join(header_words):
                    print(f"✅ MATCH PARCIAL: '{header}' contiene '{search_word}' en posición {i}")
                    return i
        
        # ESTRATEGIA 4: Casos específicos conocidos
        specific_mappings = {
            'math_score': ['math score', 'math', 'mathematics score'],
            'reading_score': ['reading score', 'reading'],
            'writing_score': ['writing score', 'writing'],
            'gender': ['gender', 'sex'],
            'race': ['race', 'ethnicity', 'race/ethnicity'],
            'education': ['education', 'parental level', 'parental level of education'],
            'lunch': ['lunch'],
            'test_prep': ['test preparation', 'test prep', 'preparation course']
        }
        
        search_base = search_name.replace('_score', '').replace('_', ' ')
        
        for mapping_key, possible_matches in specific_mappings.items():
            if search_name.startswith(mapping_key.replace('_', '')):
                for i, header in enumerate(headers):
                    header_lower = header.strip().lower()
                    for possible_match in possible_matches:
                        if possible_match in header_lower:
                            print(f"✅ MATCH ESPECÍFICO: '{header}' coincide con patrón '{possible_match}' en posición {i}")
                            return i
        
        # ESTRATEGIA 5: Índice numérico
        try:
            index = int(column_name)
            if 0 <= index < len(headers):
                print(f"✅ ÍNDICE NUMÉRICO: usando posición {index} para '{headers[index]}'")
                return index
        except ValueError:
            pass
        
        # ERROR: No encontrado
        print(f"❌ COLUMNA NO ENCONTRADA")
        print(f"💡 Sugerencias basadas en similitud:")
        
        # Mostrar sugerencias
        suggestions = []
        for i, header in enumerate(headers):
            header_lower = header.lower()
            similarity_score = 0
            
            # Calcular similaridad básica
            for word in search_words:
                if word in header_lower:
                    similarity_score += 1
            
            if similarity_score > 0:
                suggestions.append((similarity_score, i, header))
        
        # Ordenar por similaridad
        suggestions.sort(reverse=True, key=lambda x: x[0])
        
        for score, idx, header in suggestions[:3]:
            print(f"   - '{header}' (posición {idx}) - similaridad: {score}")
        
        if suggestions:
            best_match = suggestions[0]
            print(f"🎯 USANDO MEJOR COINCIDENCIA: '{best_match[2]}' en posición {best_match[1]}")
            return best_match[1]
        
        raise ValueError(f"Columna '{column_name}' no encontrada. Columnas disponibles: {headers}")
    
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

    # ========== FUNCIONES DE DEBUG ADICIONALES ==========
    
    def debug_where_issue_complete(self, query: str):
        """
        Debug completo del problema WHERE
        """
        print(f"\n{'='*80}")
        print(f"🔍 DEBUG COMPLETO WHERE CLAUSE")
        print(f"{'='*80}")
        print(f"📋 Query original: {query}")
        
        # 1. Parsear la consulta
        equality_pattern = r'select\s+\*\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)'
        match = re.search(equality_pattern, query, re.IGNORECASE)
        
        if not match:
            print("❌ ERROR: Query no coincide con patrón WHERE")
            return
        
        table_name = match.group(1)
        column_name = match.group(2)
        value = match.group(3).strip().strip('"\'')
        
        print(f"📋 Tabla: {table_name}")
        print(f"📋 Columna buscada: '{column_name}'")
        print(f"📋 Valor buscado: '{value}'")
        
        # 2. Verificar tabla
        if table_name not in self.engine.tables:
            print(f"❌ ERROR: Tabla '{table_name}' no encontrada")
            return
        
        # 3. Obtener headers y mostrar información
        headers = self.engine.get_table_headers(table_name)
        print(f"📋 Headers disponibles ({len(headers)}): {headers}")
        
        # 4. Encontrar índice de columna CON DEBUG DETALLADO
        print(f"\n🔍 PASO 1: Buscando índice de columna...")
        try:
            column_index = self._get_column_index_from_table(table_name, column_name)
            print(f"✅ Índice encontrado: {column_index}")
            if column_index < len(headers):
                print(f"✅ Columna real: '{headers[column_index]}'")
            else:
                print(f"❌ ÍNDICE FUERA DE RANGO: {column_index} >= {len(headers)}")
        except Exception as e:
            print(f"❌ ERROR obteniendo índice: {e}")
            return
        
        # 5. Obtener muestra de datos RAW
        print(f"\n🔍 PASO 2: Analizando datos raw...")
        idx = self.engine.tables[table_name]
        raw_records = idx.scan_all()[:10]  # Primeros 10
        
        print(f"📊 Tipo de datos raw: {type(raw_records)}")
        print(f"📊 Cantidad de registros: {len(raw_records)}")
        
        for i, record in enumerate(raw_records[:3]):  # Solo primeros 3
            print(f"\n📊 Registro {i}:")
            print(f"   Tipo: {type(record)}")
            print(f"   Contenido: {record}")
            
            # Intentar extraer valor de la columna objetivo
            try:
                if isinstance(record, list):
                    if column_index < len(record):
                        col_value = str(record[column_index]).strip()
                        print(f"   ✅ Valor en columna {column_index}: '{col_value}'")
                        print(f"   ✅ ¿'{col_value}' == '{value}'? {col_value == value}")
                    else:
                        print(f"   ❌ Columna {column_index} fuera de rango (len={len(record)})")
                elif isinstance(record, str):
                    import csv
                    import io
                    try:
                        reader = csv.reader(io.StringIO(record.strip()))
                        cols = next(reader, [])
                        print(f"   📊 Columnas parseadas: {cols}")
                        if column_index < len(cols):
                            col_value = cols[column_index].strip()
                            print(f"   ✅ Valor en columna {column_index}: '{col_value}'")
                            print(f"   ✅ ¿'{col_value}' == '{value}'? {col_value == value}")
                        else:
                            print(f"   ❌ Columna {column_index} fuera de rango (len={len(cols)})")
                    except Exception as parse_e:
                        print(f"   ❌ Error parseando CSV: {parse_e}")
                        # Fallback split
                        cols = record.split(',')
                        print(f"   📊 Fallback split: {cols}")
                        if column_index < len(cols):
                            col_value = cols[column_index].strip()
                            print(f"   ✅ Valor en columna {column_index}: '{col_value}'")
                            print(f"   ✅ ¿'{col_value}' == '{value}'? {col_value == value}")
            except Exception as e:
                print(f"   ❌ Error extrayendo valor: {e}")
        
        print(f"{'='*80}")