# engine.py - CORREGIDO para solucionar errores de tipos y compatibilidad con ISAM

import csv
import os
from typing import List, Any, Tuple, Dict, Union
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
        self.table_schemas: Dict[str, List[tuple]] = {}  # NUEVO: Guardar schemas para ISAM

    def _init_index(self, tipo: str, table: str, index_field: int, schema: Any) -> BaseIndex:
        """Inicializa un índice según su tipo"""
        if tipo == 'sequential':
            return SequentialFile(f'{table}_data.bin',
                                f'{table}_aux.bin',
                                field_index=index_field)
        elif tipo == 'isam':
            return ISAM(f'{table}_data.bin',
                        f'{table}_index.bin',
                        schema,
                        index_field)
        elif tipo == 'hash':
            # CORREGIDO: Hash Extensible con configuración apropiada
            hash_idx = ExtendibleHash(
                dir_file=f'indices/{table}_hash_dir.pkl',
                data_file=f'indices/{table}_hash_data.bin'
            )
            hash_idx.field_index = index_field  # Configurar campo indexado
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
            idx.load_csv(path)  # Pasar string path, no lista
            
        elif tipo == 'isam':
            # ISAM con esquema - CORREGIDO
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.reader(f)
                headers_isam = next(reader)
                rows = list(reader)

            schema = [(f'col{i}', '20s', 20) for i in range(len(headers_isam))]
            self.table_schemas[table] = schema  # NUEVO: Guardar schema
            
            data_dicts = [dict(zip([f'col{i}' for i in range(len(row))], row)) for row in rows]
            
            idx = self._init_index(tipo, table, index_field, schema)
            # Para ISAM, pasar los diccionarios, no el path
            if hasattr(idx, 'load_csv'):
                idx.load_csv(data_dicts)  # type: ignore

        elif tipo == 'hash':
            # CORREGIDO: Hash Extensible manejo específico
            idx = self._init_index(tipo, table, index_field, None)
            # Configurar el campo indexado ANTES de cargar datos
            idx.field_index = index_field
            idx.load_csv(path)  # Solo pasar path

        elif tipo == 'bplustree':
            # B+ Tree
            idx = self._init_index(tipo, table, index_field, None)
            idx.field_index = index_field
            idx.load_csv(path)

        else:
            # Otros índices (sequential)
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.reader(f)
                rows = list(reader)
            
            idx = self._init_index(tipo, table, index_field, None)
            
            if hasattr(idx, 'load_csv') and tipo != 'bplustree':
                idx.load_csv(path)
            else:
                for row in rows:
                    idx.insert(None, row)

        # Guardar índice en tabla
        self.tables[table] = idx
        
        # Mensaje de resultado con estadísticas específicas para hash
        headers_count = len(self.table_headers.get(table, []))
        tipo_real = type(idx).__name__
        
        # Información adicional por tipo de índice
        extra_info = ""
        
        if isinstance(idx, ExtendibleHash):
            # NUEVO: Estadísticas de Hash
            if hasattr(idx, 'get_stats'):
                stats = idx.get_stats()
                extra_info = f" (Profundidad global: {stats['global_depth']}, {stats['total_records']} registros, {stats['total_buckets']} buckets)"
            
        elif RTREE_AVAILABLE and MultidimensionalRTree and isinstance(idx, MultidimensionalRTree):
            # Usar getattr para acceso seguro al atributo
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
        """Obtener información completa de una tabla incluyendo headers"""
        if table_name not in self.tables:
            return {}
        
        index = self.tables[table_name]
        
        # NUEVO: Información específica para Hash
        info = {
            'name': table_name,
            'index_type': type(index).__name__,
            'headers': self.get_table_headers(table_name),
            'csv_path': self.get_table_file_path(table_name),
            'field_index': getattr(index, 'field_index', None),
            'headers_count': len(self.get_table_headers(table_name))
        }
        
        # Agregar estadísticas específicas para Hash
        if isinstance(index, ExtendibleHash) and hasattr(index, 'get_stats'):
            info['hash_stats'] = index.get_stats()
        
        return info
    
    def list_all_tables_info(self) -> Dict[str, dict]:
        """Obtener información de todas las tablas cargadas"""
        return {table_name: self.get_table_info(table_name) for table_name in self.tables.keys()}

    # ========== MÉTODOS PRINCIPALES ==========

    def _format_record_to_csv(self, record: Any) -> str:
        """Convierte cualquier registro a formato CSV (separado por comas)"""
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
                cleaned = f'"{cleaned.replace('"', '""')}"'
            cleaned_values.append(cleaned)
        
        return ','.join(cleaned_values)

    def _list_to_isam_dict(self, table: str, values: List[str]) -> Dict[str, Any]:
        """Convierte una lista de valores a diccionario para ISAM"""
        if table not in self.table_schemas:
            # Generar schema básico si no existe
            schema = [(f'col{i}', '20s', 20) for i in range(len(values))]
            self.table_schemas[table] = schema
        else:
            schema = self.table_schemas[table]
        
        # Crear diccionario usando las claves del schema
        result = {}
        for i, (field_name, _, _) in enumerate(schema):
            if i < len(values):
                result[field_name] = values[i]
            else:
                result[field_name] = ""  # Valor por defecto para campos faltantes
        
        return result

    def insert(self, table: str, values: List[str]) -> str:
        """Insertar un registro en una tabla - CORREGIDO para ISAM"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        idx = self.tables[table]
        
        # NUEVA LÓGICA: Manejo especial para ISAM
        if isinstance(idx, ISAM):
            # Convertir lista a diccionario para ISAM
            val_dict = self._list_to_isam_dict(table, values)
            idx.insert(None, val_dict)
        else:
            # Para otros índices (incluido Hash), usar el método original
            idx.insert(None, values)
        
        return f"Registro insertado en '{table}'"

    def scan(self, table: str) -> str:
        """Escanear tabla completa con manejo específico por tipo de índice"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        idx = self.tables[table]
        registros = idx.scan_all()
        
        # Manejo específico para R-Tree - CORREGIDO
        if RTREE_AVAILABLE and MultidimensionalRTree and isinstance(idx, MultidimensionalRTree):
            formatted_records = []
            for vector, obj in registros:
                if isinstance(obj, list):
                    # Formatear cada valor como CSV limpio
                    cleaned_values = []
                    for v in obj:
                        cleaned = str(v).strip()
                        if ',' in cleaned or '"' in cleaned or '\n' in cleaned:
                            cleaned = f'"{cleaned.replace('"', '""')}"'
                        cleaned_values.append(cleaned)
                    csv_record = ','.join(cleaned_values)
                    formatted_records.append(csv_record)
                else:
                    csv_record = self._format_record_to_csv(obj)
                    formatted_records.append(csv_record)
            return '\n'.join(formatted_records)
        
        # Manejo para otros tipos de índices (incluye Hash mejorado)
        formatted_records = []
        for record in registros:
            csv_record = self._format_record_to_csv(record)
            formatted_records.append(csv_record)
        
        return '\n'.join(formatted_records)

   # engine.py - REEMPLAZAR COMPLETAMENTE el método search con esta versión:

    def search(self, table: str, key: str, column: int) -> List[str]:
        """Buscar registros y devolverlos en formato CSV - VERSIÓN CORREGIDA PARA SEQUENTIAL FILE"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        idx = self.tables[table]

        print(f"\n{'='*60}")
        print(f"🔍 SEARCH DEBUG - Buscando en tabla '{table}'")
        print(f"   - Clave: '{key}'")
        print(f"   - Columna: {column}")
        print(f"   - Tipo de índice: {type(idx).__name__}")
        
        # Obtener headers para referencia
        headers = self.get_table_headers(table)
        if column < len(headers):
            print(f"   - Nombre de columna: '{headers[column]}'")
        
        # OPTIMIZACIÓN: Hash con búsqueda directa (solo si coincide el field_index)
        if (hasattr(idx, 'search') and hasattr(idx, 'field_index') and 
            idx.field_index == column and isinstance(idx, ExtendibleHash)):
            try:
                print(f"🚀 Usando búsqueda directa del Hash (field_index={idx.field_index})")
                resultados = idx.search(key)
                final_result = []
                for r in resultados:
                    csv_record = self._format_record_to_csv(r)
                    final_result.append(csv_record)
                print(f"✅ Búsqueda directa encontró {len(final_result)} registros")
                print(f"{'='*60}")
                return final_result
            except Exception as e:
                print(f"❌ Error en búsqueda directa: {e}")
        
        # FULL SCAN con filtro manual - VERSIÓN CORREGIDA
        print(f"🔄 Usando full scan con filtro manual")
        resultados = []
        all_records = idx.scan_all()
        total_records = len(all_records)
        print(f"📊 Escaneando {total_records} registros totales")
        
        # Caso especial para R-Tree
        if RTREE_AVAILABLE and MultidimensionalRTree and isinstance(idx, MultidimensionalRTree):
            print(f"🗺️ Procesando R-Tree...")
            for vector, obj in all_records:
                if isinstance(obj, list) and column < len(obj):
                    obj_value = str(obj[column]).strip()
                    if obj_value == str(key).strip():
                        cleaned_values = []
                        for v in obj:
                            cleaned = str(v).strip()
                            if ',' in cleaned or '"' in cleaned or '\n' in cleaned:
                                cleaned = f'"{cleaned.replace('"', '""')}"'
                            cleaned_values.append(cleaned)
                        csv_record = ','.join(cleaned_values)
                        resultados.append(csv_record)
            print(f"✅ R-Tree encontró {len(resultados)} registros")
            print(f"{'='*60}")
            return resultados
        
        # PARA OTROS ÍNDICES - LÓGICA CORREGIDA PARA SEQUENTIAL FILE
        matches_found = 0
        
        for row_index, row in enumerate(all_records):
            try:
                # Extraer valor de la columna objetivo
                cell_value = None
                
                if isinstance(row, dict):
                    # Diccionario (ISAM)
                    values = list(row.values())
                    if column < len(values):
                        cell_value = str(values[column]).strip()
                        
                elif isinstance(row, (list, tuple)):
                    # Lista/tupla (Hash, B+Tree)
                    if column < len(row):
                        cell_value = str(row[column]).strip()
                        
                elif isinstance(row, str):
                    # String - CORREGIDO PARA SEQUENTIAL FILE
                    import csv
                    import io
                    
                    # DETECCIÓN AUTOMÁTICA DEL FORMATO
                    if '|' in row:
                        # Sequential File usa separador |
                        cols = [c.strip() for c in row.split('|')]
                        if column < len(cols):
                            cell_value = cols[column].strip()
                    else:
                        # Otros índices usan formato CSV
                        try:
                            # Usar csv.reader para parsing robusto
                            reader = csv.reader(io.StringIO(row.strip()))
                            cols = next(reader, [])
                            if column < len(cols):
                                cell_value = cols[column].strip()
                        except Exception:
                            # Fallback: split por comas
                            cols = [c.strip() for c in row.split(',')]
                            if column < len(cols):
                                cell_value = cols[column].strip()
                
                else:
                    # Tipo desconocido
                    print(f"⚠️ Tipo de registro desconocido: {type(row)}")
                    continue
                
                # DEBUG para los primeros 5 registros
                if row_index < 5:
                    separator = "|" if isinstance(row, str) and "|" in row else ","
                    print(f"🔍 Row {row_index} (sep='{separator}'): column[{column}]='{cell_value}' vs key='{key}' -> match={cell_value == str(key).strip() if cell_value else False}")
                
                # COMPARACIÓN EXACTA
                if cell_value is not None and cell_value == str(key).strip():
                    csv_record = self._format_record_to_csv(row)
                    resultados.append(csv_record)
                    matches_found += 1
                    
                    # Debug para coincidencias
                    if matches_found <= 3:  # Mostrar las primeras 3 coincidencias
                        print(f"✅ COINCIDENCIA {matches_found}: {csv_record[:100]}...")
                    
            except Exception as e:
                if row_index < 5:  # Solo mostrar errores para los primeros registros
                    print(f"❌ Error procesando row {row_index}: {e}")
                continue

        print(f"📊 RESULTADO FINAL: {matches_found} registros coinciden de {total_records} totales")
        print(f"{'='*60}")
        return resultados

    def range_search(self, table: str, begin_key: str, end_key: str) -> List[str]:
        """Búsqueda por rango con soporte específico para R-Tree espacial"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        idx = self.tables[table]

        # ——— MANEJO ESPECÍFICO PARA R-TREE - CORREGIDO ———
        if RTREE_AVAILABLE and MultidimensionalRTree and isinstance(idx, MultidimensionalRTree):
            try:
                # Parsear coordenadas del punto
                point = [float(x.strip()) for x in begin_key.split(',')]
                
                # Determinar si es radio (float) o KNN (int)
                try:
                    if '.' in str(end_key):
                        param = float(end_key)
                    else:
                        param = int(end_key)
                except ValueError:
                    raise ValueError("Parámetro inválido para R-Tree")
                
                # Ejecutar búsqueda espacial
                spatial_results = idx.range_search(point, param)
                
                # Formatear resultados con distancia
                formatted_results = []
                for dist, obj in spatial_results:
                    if isinstance(obj, list):
                        # Limpiar valores CSV
                        cleaned_values = []
                        for v in obj:
                            cleaned = str(v).strip()
                            if ',' in cleaned or '"' in cleaned or '\n' in cleaned:
                                cleaned = f'"{cleaned.replace('"', '""')}"'
                            cleaned_values.append(cleaned)
                        
                        # Agregar distancia al final
                        csv_record = ','.join(cleaned_values) + f',{dist:.3f}'
                        formatted_results.append(csv_record)
                    else:
                        # Fallback para objetos no-lista
                        csv_record = self._format_record_to_csv(obj)
                        formatted_results.append(f"{csv_record},{dist:.3f}")
                
                return formatted_results
                
            except Exception as e:
                raise ValueError(f"Error en búsqueda espacial R-Tree: {e}")
        
        # NUEVO: Hash Extensible NO soporta rangos (comportamiento correcto)
        if isinstance(idx, ExtendibleHash):
            raise ValueError("Hash Extensible no soporta búsquedas por rango. Use ISAM o B+ Tree para rangos.")
        
        # ——— OTROS TIPOS DE ÍNDICES ———
        if hasattr(idx, 'range_search'):
            raw_results = idx.range_search(begin_key, end_key)
            return [self._format_record_to_csv(r) for r in raw_results]
        
        # ——— FALLBACK CONTROLADO ———
        if not hasattr(idx, 'field_index'):
            raise ValueError(f"Índice {type(idx).__name__} no soporta range_search")
        
        resultados: List[str] = []
        field_idx = getattr(idx, 'field_index', 0)
        
        for row in idx.scan_all():
            try:
                # Extraer valor de comparación
                if isinstance(row, dict):
                    values = list(row.values())
                    val = str(values[field_idx]) if field_idx < len(values) else ""
                elif isinstance(row, (list, tuple)):
                    val = str(row[field_idx]) if field_idx < len(row) else ""
                elif isinstance(row, str):
                    cols = row.split('|' if '|' in row else ',')
                    val = cols[field_idx].strip() if field_idx < len(cols) else ""
                else:
                    val = str(row)
                
                # Aplicar filtro de rango
                if begin_key <= val <= end_key:
                    csv_record = self._format_record_to_csv(row)
                    resultados.append(csv_record)
                    
            except (IndexError, ValueError):
                continue
        
        return resultados

    def remove(self, table: str, key: str) -> List[str]:
        """Eliminar registros con formato CSV consistente"""
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        idx = self.tables[table]
        
        if hasattr(idx, 'remove'):
            try:
                raw_results = idx.remove(key)
                
                # NUEVO: Hash devuelve strings CSV directamente, otros necesitan formateo
                if isinstance(idx, ExtendibleHash):
                    return raw_results  # Ya están en formato CSV
                else:
                    # Otros índices devuelven objetos que necesitan formateo
                    return [self._format_record_to_csv(r) for r in raw_results]
            except Exception as e:
                raise ValueError(f"Error eliminando registros: {e}")
        
        raise NotImplementedError("El índice no soporta eliminación")