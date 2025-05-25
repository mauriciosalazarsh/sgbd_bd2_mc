# engine.py
import csv

from typing import List, Any, Tuple, Dict
from typing import Any
from indices.sequential import SequentialFile
from indices.isam import ISAM
from indices.hash_extensible import ExtendibleHash
from indices.btree import BPlusTree
from indices.rtree import MultidimensionalRTree
from indices.base_index import BaseIndex

class Engine:
    def __init__(self):
        self.tables: dict[str, BaseIndex] = {}

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
        if tipo == 'hash':
            return ExtendibleHash(
                dir_file=f'indices/{table}_dir.pkl',
                data_file=f'data/{table}_data.bin'
            )
        elif tipo == 'bplustree':
            return BPlusTree(f'{table}_btree.pkl')
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

        if tipo == 'isam':
            # Leer encabezado y filas como diccionario
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.reader(f)
                headers = next(reader)
                rows = list(reader)

            schema = [(f'col{i}', '20s', 20) for i in range(len(headers))]
            data_dicts = [dict(zip([f'col{i}' for i in range(len(row))], row)) for row in rows]

            idx = self._init_index(tipo, table, index_field, schema)
            idx.load_csv(data_dicts)  # type: ignore

        else:
            # Para índices que no usan esquema (como sequential)
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.reader(f)
                rows = list(reader)
            schema = None
            idx = self._init_index(tipo, table, index_field, schema)

            if hasattr(idx, 'load_csv'):
                idx.load_csv(path)
            else:
                for row in rows:
                    idx.insert(None, row)

        self.tables[table] = idx
        return f"Tabla '{table}' cargada con éxito usando índice {tipo}"


    def insert(self, table: str, values: List[str]) -> str:
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        self.tables[table].insert(None, values)
        return f"Registro insertado en '{table}'"

    def scan(self, table: str) -> str:
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        
        registros = self.tables[table].scan_all()

        # Convertir dicts a string si es necesario
        def to_str(r: Any) -> str:
            if isinstance(r, dict):
                return ' | '.join(str(v) for v in r.values())
            return str(r)

        return '\n'.join(to_str(r) for r in registros)



    def search(self,
            table: str,
            key: str,
            column: int) -> List[str]:
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
                    if column < len(values) and values[column] == key:
                        resultados.append(' | '.join(str(v) for v in values))
                elif isinstance(row, str):
                    cols = [c.strip() for c in row.split('|')]
                    if column < len(cols) and cols[column] == key:
                        resultados.append(row)

            return resultados

        # convertir los resultados a string (para ambos casos)
        final_result = []
        for r in resultados:
            if isinstance(r, dict):
                final_result.append(' | '.join(str(v) for v in r.values()))
            else:
                final_result.append(str(r))
        return final_result


    def range_search(self,
                        table: str,
                        begin_key: str,
                        end_key: str) -> List[str]:
            """
            Retorna todas las filas cuya columna indexada esté entre begin_key y end_key.
            Sólo funciona si el índice soporta `range_search()`. Si no, tendrías que
            full-scan + comparar.
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
                # formatear: [(dist, obj)] → ["obj (dist=...)"]
                return [f"{obj} (dist={dist:.3f})" for dist, obj in results]
            # ————————————————————————————————

            # tu implementación original para los demás índices
            if hasattr(idx, 'range_search'):
                return idx.range_search(begin_key, end_key)

            # fallback naive
            resultados: List[str] = []
            for row_str in idx.scan_all():
                cols = [c.strip() for c in row_str.split('|')]
                val = cols[idx.field_index]
                if begin_key <= val <= end_key:
                    resultados.append(row_str)
            return resultados

    def remove(self,
               table: str,
               key: str) -> List[str]:
        """
        Elimina (o marca como eliminado) todos los registros con `key`
        en la columna indexada. Depende de que el índice tenga un método remove().
        """
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no encontrada")
        idx = self.tables[table]
        if hasattr(idx, 'remove'):
            return idx.remove(key)
        raise NotImplementedError("El índice no soporta eliminación")



        # 2) Aplica el resto de condiciones con full‐scan
        filtered: List[str] = []
        for row in base_rows:
            cols = [c.strip() for c in row.split('|')]
            if all(col < len(cols) and cols[col] == val
                   for col, val in conditions):
                filtered.append(row)
        return filtere