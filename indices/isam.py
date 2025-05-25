# indices/isam.py

import os
import struct
from typing import List, Dict, Any
from indices.base_index import BaseIndex

class RegistroGenerico:
    def __init__(self, schema: List[tuple]):
        # schema: list of (campo, tipo_bytes, longitud_bytes)
        self.schema = schema
        self.format = ''.join([t[1] for t in schema])
        self.size = struct.calcsize(self.format)

    def pack(self, valores: Dict[str, Any]) -> bytes:
        datos = []
        for campo, tipo, tam in self.schema:
            val = valores[campo]
            if tipo.endswith('s'):
                b = str(val).encode('latin1')
                datos.append(b.ljust(int(tipo[:-1]), b' ')[:int(tipo[:-1])])
            else:
                datos.append(int(val))
        return struct.pack(self.format, *datos)

    def unpack(self, data: bytes) -> Dict[str, Any]:
        raw = struct.unpack(self.format, data)
        resultado: Dict[str, Any] = {}
        for i, (campo, tipo, tam) in enumerate(self.schema):
            if tipo.endswith('s'):
                resultado[campo] = raw[i].decode('latin1').rstrip(' ')
            else:
                resultado[campo] = raw[i]
        return resultado

class ISAM(BaseIndex):
    def __init__(
        self,
        data_file: str,
        index_file: str,
        schema: List[tuple],
        index_field: int,
        block_factor_data: int = 5,
        block_factor_index: int = 5
    ):
        self.data_file = data_file
        self.index_lvl1_file = index_file
        self.index_lvl2_file = f"{index_file}.lvl2"
        self.overflow_file = f"{data_file}.ovf"
        self.reg = RegistroGenerico(schema)
        self.index_field = index_field
        self.block_factor_data = block_factor_data
        self.block_factor_index = block_factor_index

        # Inicializar archivos si no existen
        if not os.path.exists(self.data_file):
            with open(self.data_file, 'wb') as f:
                f.write(struct.pack('i', 0))
        if not os.path.exists(self.index_lvl1_file):
            open(self.index_lvl1_file, 'wb').close()
        if not os.path.exists(self.index_lvl2_file):
            open(self.index_lvl2_file, 'wb').close()
        if not os.path.exists(self.overflow_file):
            with open(self.overflow_file, 'wb') as f:
                f.write(struct.pack('i', 0))

    def _read_header(self) -> int:
        with open(self.data_file, 'rb') as f:
            return struct.unpack('i', f.read(4))[0]

    def _write_header(self, total: int) -> None:
        with open(self.data_file, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('i', total))

    def _read_record(self, pos: int) -> Dict[str, Any]:
        with open(self.data_file, 'rb') as f:
            f.seek(4 + pos * self.reg.size)
            data = f.read(self.reg.size)
        return self.reg.unpack(data)

    def _write_record(self, valores: Dict[str, Any], pos: int) -> None:
        with open(self.data_file, 'r+b') as f:
            f.seek(4 + pos * self.reg.size)
            f.write(self.reg.pack(valores))

    def _append_index_lvl1_entry(self, valores: Dict[str, Any]) -> None:
        with open(self.index_lvl1_file, 'ab') as f:
            f.write(self.reg.pack(valores))

    def _append_index_lvl2_entry(self, valores: Dict[str, Any]) -> None:
        with open(self.index_lvl2_file, 'ab') as f:
            f.write(self.reg.pack(valores))

    def _read_index_lvl1(self) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        with open(self.index_lvl1_file, 'rb') as f:
            while True:
                data = f.read(self.reg.size)
                if not data:
                    break
                entries.append(self.reg.unpack(data))
        return entries

    def _read_index_lvl2(self) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        with open(self.index_lvl2_file, 'rb') as f:
            while True:
                data = f.read(self.reg.size)
                if not data:
                    break
                entries.append(self.reg.unpack(data))
        return entries

    def _read_overflow_header(self) -> int:
        with open(self.overflow_file, 'rb') as f:
            return struct.unpack('i', f.read(4))[0]

    def _write_overflow_header(self, total: int) -> None:
        with open(self.overflow_file, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('i', total))

    def _append_overflow(self, valores: Dict[str, Any]) -> None:
        with open(self.overflow_file, 'ab') as f:
            f.write(self.reg.pack(valores))

    def load_csv(self, rows: List[Dict[str, Any]]) -> None:
        # Ordenar según campo de índice
        rows.sort(key=lambda r: list(r.values())[self.index_field])

        # Reset archivos: data, índices y overflow
        with open(self.data_file, 'wb') as f:
            f.write(struct.pack('i', len(rows)))
        open(self.index_lvl1_file, 'wb').close()
        open(self.index_lvl2_file, 'wb').close()
        with open(self.overflow_file, 'wb') as f:
            f.write(struct.pack('i', 0))

        # Escribir registros en data y entradas en índice nivel 1
        with open(self.data_file, 'r+b') as f:
            f.seek(4)
            for i, row in enumerate(rows):
                f.write(self.reg.pack(row))
                if i % self.block_factor_data == 0:
                    self._append_index_lvl1_entry(row)

        # Escribir entradas en índice nivel 2 a partir del nivel 1
        lvl1_entries = self._read_index_lvl1()
        for j, entry in enumerate(lvl1_entries):
            if j % self.block_factor_index == 0:
                self._append_index_lvl2_entry(entry)

    def insert(self, _: None, val_dict: Dict[str, Any]) -> None:
        # Inserción dinámica a overflow
        ov_total = self._read_overflow_header()
        self._write_overflow_header(ov_total + 1)
        self._append_overflow(val_dict)

    def search(self, valor: Any) -> List[Dict[str, Any]]:
        key = str(valor)
        lvl2 = self._read_index_lvl2()
        lvl1 = self._read_index_lvl1()

        # Seleccionar bloque de nivel 1 usando nivel 2
        if lvl2:
            low, high = 0, len(lvl2) - 1
            l2_block = 0
            while low <= high:
                mid = (low + high) // 2
                mid_key = str(list(lvl2[mid].values())[self.index_field])
                if mid_key <= key:
                    l2_block = mid
                    low = mid + 1
                else:
                    high = mid - 1
            start_l1 = l2_block * self.block_factor_index
            end_l1 = min(start_l1 + self.block_factor_index, len(lvl1))
            subset = lvl1[start_l1:end_l1]
        else:
            subset = lvl1
            start_l1 = 0

        # Seleccionar bloque de datos usando nivel 1
        if subset:
            low, high = 0, len(subset) - 1
            l1_block = 0
            while low <= high:
                mid = (low + high) // 2
                mid_key = str(list(subset[mid].values())[self.index_field])
                if mid_key <= key:
                    l1_block = mid
                    low = mid + 1
                else:
                    high = mid - 1
            data_block = start_l1 + l1_block
        else:
            data_block = 0

        results: List[Dict[str, Any]] = []
        total = self._read_header()
        start = data_block * self.block_factor_data
        for i in range(start, min(start + self.block_factor_data, total)):
            rec = self._read_record(i)
            if str(list(rec.values())[self.index_field]) == key:
                results.append(rec)

        # Buscar también en overflow
        ov_total = self._read_overflow_header()
        with open(self.overflow_file, 'rb') as f:
            f.seek(4)
            for _ in range(ov_total):
                data = f.read(self.reg.size)
                rec = self.reg.unpack(data)
                if str(list(rec.values())[self.index_field]) == key:
                    results.append(rec)

        return results

    def range_search(self, begin_key: Any, end_key: Any) -> List[str]:
        begin_key = str(begin_key)
        end_key = str(end_key)
        results: List[str] = []
        total = self._read_header()
        for i in range(total):
            rec = self._read_record(i)
            val = str(list(rec.values())[self.index_field])
            if begin_key <= val <= end_key:
                results.append(' | '.join(str(v) for v in rec.values()))

        ov_total = self._read_overflow_header()
        with open(self.overflow_file, 'rb') as f:
            f.seek(4)
            for _ in range(ov_total):
                data = f.read(self.reg.size)
                rec = self.reg.unpack(data)
                val = str(list(rec.values())[self.index_field])
                if begin_key <= val <= end_key:
                    results.append(' | '.join(str(v) for v in rec.values()))

        return results

    def scan_all(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        total = self._read_header()
        for i in range(total):
            results.append(self._read_record(i))

        ov_total = self._read_overflow_header()
        with open(self.overflow_file, 'rb') as f:
            f.seek(4)
            for _ in range(ov_total):
                data = f.read(self.reg.size)
                results.append(self.reg.unpack(data))

        return results

    def remove(self, key: Any) -> List[str]:
        key_str = str(key)
        all_records = self.scan_all()
        kept: List[Dict[str, Any]] = []
        removed: List[Dict[str, Any]] = []
        for rec in all_records:
            if str(list(rec.values())[self.index_field]) == key_str:
                removed.append(rec)
            else:
                kept.append(rec)

        # Reconstruir archivos con registros restantes
        self.load_csv(kept)
        return [' | '.join(str(v) for v in rec.values()) for rec in removed]
