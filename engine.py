# engine.py (ACTUALIZADO con formato estandarizado)
import csv
import os
from typing import List, Any, Tuple, Dict
from indices.sequential import SequentialFile
from indices.isam import ISAM
from indices.hash_extensible import ExtendibleHash
from indices.btree import BPlusTree
from indices.rtree import MultidimensionalRTree
from indices.base_index import BaseIndex

class Engine:
    def __init__(self):
        self.tables: dict[str, BaseIndex] = {}
        self.table_headers: dict[str, List[str]] = {}      # ← NUEVO: Guardar headers por tabla
        self.table_file_paths: dict[str, str] = {}         # ← NUEVO: Guardar paths de archivos CSV

    def _init_index(self, tipo: str, table: str, index_field: int, schema: Any) -> BaseIndex:
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
            return ExtendibleHash(
                dir_file=f'indices/{table}_dir.pkl',
                data_file=f'data/{table}_data.bin'
            )
        elif tipo == 'bplustree':
            tree = BPlusTree(f'{table}_btree.pkl')
            tree.field_index = index_field  # ← para que pueda hacer búsqueda por columna
            return tree
        elif tipo == 'rtree':
            return MultidimensionalRTree(path=f'{table}_rtree',
                                        dimension=2)
        else:
            raise ValueError(f"Tipo de índice '{tipo}' no soportado")

    def load_csv(self,
                table: str,
                path: str,
                tipo: str,
                index_field: int) -> str:
        print(f"Intentando leer archivo: {path}")

        # ========== NUEVO: LEER Y GUARDAR HEADERS PRIMERO ==========
        try:
            headers = []
            with open(path, 'r', encoding='latin1') as f:
                reader = csv.reader(f)
                first_row = next(reader, [])
                headers = [col.strip() for col in first_row if col.strip()]
            
            # Guardar headers y path del archivo
            self.table_headers[table] = headers
            self.table_file_paths[table] = path
            
            print(f"Headers detectados para tabla '{table}': {headers}")
            print(f"Total de columnas: {len(headers)}")
            
        except Exception as e:
            print(f"Advertencia: No se pudieron leer headers del archivo {path}: {e}")
            self.table_headers[table] = []
            self.table_file_paths[table] = path

        # ========== CÓDIGO ORIGINAL (sin cambios) ==========
        if tipo == 'isam':
            # Leer encabezado y filas como diccionario
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.reader(f)
                headers_isam = next(reader)  # Renamed to avoid conflict
                rows = list(reader)

            schema = [(f'col{i}', '20s', 20) for i in range(len(headers_isam))]
            data_dicts = [dict(zip([f'col{i}' for i in range(len(row))], row)) for row in rows]

            idx = self._init_index(tipo, table, index_field, schema)
            idx.load_csv(data_dicts)  # type: ignore

        elif tipo == 'bplustree':
            # Para B+ Tree, establecer el field_index y usar load_csv simple
            idx = self._init_index(tipo, table, index_field, None)
            # Establecer la columna a indexar antes de cargar
            idx.field_index = index_field
            idx.load_csv(path)

        else:
            # Para índices que no usan esquema (como sequential)
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.reader(f)
                rows = list(reader)
            schema = None
            idx = self._init_index(tipo, table, index_field, schema)

            if hasattr(idx, 'load_csv') and tipo != 'bplustree':
                idx.load_csv(path)
            else:
                for row in rows:
                    idx.insert(None, row)

        self.tables[table] = idx
        
        # ========== MENSAJE DE RESULTADO MEJORADO ==========
        headers_count = len(self.table_headers.get(table, []))
        return f"Tabla '{table}' cargada con éxito usando índice {tipo}. Detectadas {headers_count} columnas: {', '.join(self.table_headers.get(table, [])[:5])}{'...' if headers_count > 5 else ''}"

    # ========== NUEVOS MÉTODOS PARA HEADERS ==========
    
    def get_table_headers(self, table_name: str) -> List[str]:
        """
        Obtener los headers/columnas de una tabla específica
        """
        return self.table_headers.get(table_name, [])
    
    def get_table_file_path(self, table_name: str) -> str:
        """
        Obtener la ruta del archivo CSV original de una tabla
        """
        return self.table_file_paths.get(table_name, '')
    
    def get_table_info(self, table_name: str) -> dict:
        """
        Obtener información completa de una tabla incluyendo headers
        """
        if table_name not in self.tables:
            return {}
        
        index = self.tables[table_name]
        
        return {
            'name': table_name,
            'index_type': type(index).__name__,
            'headers': self.get_table_headers(table_name),
            'csv_path': self.get_table_file_path(table_name),
            'field_index': getattr(index, 'field_index', None),
            'headers_count': len(self.get_table_headers(table_name))
        }
    
    def list_all_tables_info(self) -> Dict[str, dict]:
        """
        Obtener información de todas las tablas cargadas
        """
        return {table_name: self.get_table_info(table_name) for table_name in self.tables.keys()}

    # ========== MÉTODOS PRINCIPALES CON FORMATO ESTANDARIZADO ==========

    def _format_record_to_csv(self, record: Any) -> str:
        """
        Convierte cualquier registro a formato CSV (separado por comas)
        """
        if isinstance(record, dict):
            # Convertir diccionario a lista de valores
            values = [str(v) for v in record.values()]
        elif isinstance(record, (list, tuple)):
            # Ya es una lista/tupla
            values = [str(v) for v in record]
        elif isinstance(record, str):
            # Si ya es string, verificar si está separado por |
            if '|' in record:
                values = [v.strip() for v in record.split('|')]
            else:
                # Asumir que es un solo valor
                values = [record]
        else:
            # Cualquier otro tipo
            values = [str(record)]
        
        # Limpiar y formatear cada valor
        cleaned_values = []
        for v in values:
            cleaned = str(v).strip()
            # Si el valor contiene comas, espacios o comillas, envolverlo en comillas
            if ',' in cleaned or '"' in cleaned or '\n' in cleaned:
                cleaned = f'"{cleaned.replace('"', '""')}"'
            cleaned_values.append(cleaned)
        
        return ','.join(cleaned_values)

    def insert(self, table: str, values: List[str]) -> str:
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        self.tables[table].insert(None, values)
        return f"Registro insertado en '{table}'"

    def scan(self, table: str) -> str:
        """
        Obtener todos los registros en formato CSV (separado por comas)
        """
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        registros = self.tables[table].scan_all()
        
        # Convertir todos los registros a formato CSV consistente
        formatted_records = []
        for record in registros:
            csv_record = self._format_record_to_csv(record)
            formatted_records.append(csv_record)
        
        return '\n'.join(formatted_records)

    def search(self, table: str, key: str, column: int) -> List[str]:
        """
        Buscar registros y devolverlos en formato CSV
        """
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        idx = self.tables[table]

        # 1) si el índice tiene search() y es sobre la misma columna:
        if hasattr(idx, 'search') and getattr(idx, 'field_index', None) == column:
            resultados = idx.search(key)
        else:
            # 2) full-scan + filtro
            resultados = []
            for row in idx.scan_all():
                # asegurarse de que sea string o dict
                if isinstance(row, dict):
                    values = list(row.values())
                    if column < len(values) and str(values[column]) == str(key):
                        resultados.append(row)
                elif isinstance(row, (list, tuple)):
                    if column < len(row) and str(row[column]) == str(key):
                        resultados.append(row)
                elif isinstance(row, str):
                    # Parsear string existente
                    if '|' in row:
                        cols = [c.strip() for c in row.split('|')]
                    else:
                        cols = [c.strip() for c in row.split(',')]
                    if column < len(cols) and cols[column] == key:
                        resultados.append(row)

        # Convertir resultados a formato CSV consistente
        final_result = []
        for r in resultados:
            csv_record = self._format_record_to_csv(r)
            final_result.append(csv_record)
        
        return final_result

    def range_search(self, table: str, begin_key: str, end_key: str) -> List[str]:
        """
        Búsqueda por rango con formato CSV consistente
        """
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        idx = self.tables[table]

        # ——— NUEVO: caso específico para RTree ———
        if isinstance(idx, MultidimensionalRTree):
            # parsear punto "lat,lon"
            try:
                point = [float(x) for x in begin_key.split(',')]
            except Exception:
                raise ValueError("Para RTree, el begin_key debe ser 'lat,lon'")
            # parsear radio (float) o k (int)
            try:
                param = int(end_key)
            except ValueError:
                param = float(end_key)
            # llamamos directamente al RTree
            results = idx.range_search(point, param)
            # formatear: [(dist, obj)] → [csv_format]
            formatted_results = []
            for dist, obj in results:
                csv_record = self._format_record_to_csv(obj)
                formatted_results.append(f"{csv_record},distance={dist:.3f}")
            return formatted_results
        # ————————————————————————————————

        # tu implementación original para los demás índices
        if hasattr(idx, 'range_search'):
            raw_results = idx.range_search(begin_key, end_key)
            return [self._format_record_to_csv(r) for r in raw_results]

        # fallback naive
        resultados: List[str] = []
        for row in idx.scan_all():
            # Extraer valor de la columna indexada para comparar
            if isinstance(row, dict):
                values = list(row.values())
                if idx.field_index < len(values):
                    val = str(values[idx.field_index])
                else:
                    continue
            elif isinstance(row, (list, tuple)):
                if idx.field_index < len(row):
                    val = str(row[idx.field_index])
                else:
                    continue
            elif isinstance(row, str):
                if '|' in row:
                    cols = [c.strip() for c in row.split('|')]
                else:
                    cols = [c.strip() for c in row.split(',')]
                if idx.field_index < len(cols):
                    val = cols[idx.field_index]
                else:
                    continue
            else:
                val = str(row)
            
            if begin_key <= val <= end_key:
                csv_record = self._format_record_to_csv(row)
                resultados.append(csv_record)
        
        return resultados

    def remove(self, table: str, key: str) -> List[str]:
        """
        Eliminar registros con formato CSV consistente
        """
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        idx = self.tables[table]
        
        if hasattr(idx, 'remove'):
            raw_results = idx.remove(key)
            return [self._format_record_to_csv(r) for r in raw_results]
        
        raise NotImplementedError("El índice no soporta eliminación")