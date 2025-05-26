import os
import struct
import csv
from typing import List, Optional
from .base_index import BaseIndex

HEADER_SIZE = 8   # 4 bytes head + 4 bytes num_campos
MAX_AUX = 10      # límite de aux antes de rebuild

class Registro:
    def __init__(self, campos: List[str], next_pos: int = -1):
        self.campos = campos
        self.next_pos = next_pos

    def pack(self, fmt: str) -> bytes:
        vals = [c.encode('latin1') for c in self.campos] + [self.next_pos]
        return struct.pack(fmt, *vals)

    @staticmethod
    def unpack(data: bytes, fmt: str, num_campos: int) -> 'Registro':
        raw = struct.unpack(fmt, data)
        campos = []
        for b in raw[:num_campos]:
            # recorta en el primer '\x00' y luego quita espacios
            texto = b.split(b'\x00', 1)[0].decode('latin1').strip()
            campos.append(texto)
        return Registro(campos, raw[-1])

    def __str__(self) -> str:
        return ' | '.join(self.campos)

class SequentialFile(BaseIndex):
    field_index: int  # override Optional

    def __init__(self, data_file: str, aux_file: str, field_index: int = 0):
        self.data_file = data_file
        self.aux_file = aux_file
        self.field_index = field_index
        self.head = -1
        self.format = ''
        self.num_campos = 0
        if os.path.exists(data_file):
            with open(data_file, 'rb') as f:
                self.head = struct.unpack('i', f.read(4))[0]
                self.num_campos = struct.unpack('i', f.read(4))[0]
                self.format = self._gen_fmt(self.num_campos)

    def _gen_fmt(self, num_campos: int) -> str:
        return ''.join(['40s'] * num_campos) + 'i'

    def _record_size(self) -> int:
        return struct.calcsize(self.format)

    def _read_header(self, path: str) -> int:
        with open(path, 'rb') as f:
            return struct.unpack('i', f.read(4))[0]

    def _write_header(self, path: str, head: int) -> None:
        with open(path, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('i', head))

    def _write_numcampos(self, path: str) -> None:
        with open(path, 'r+b') as f:
            f.seek(4)
            f.write(struct.pack('i', self.num_campos))

    def _read_record(self, path: str, pos: int) -> Registro:
        if pos is None or pos < 0:
            raise ValueError(f"Índice inválido: {pos}")
        offset = HEADER_SIZE + pos * self._record_size()
        with open(path, 'rb') as f:
            f.seek(offset)
            data = f.read(self._record_size())
        if len(data) < self._record_size():
            raise ValueError("Registro incompleto en disco.")
        return Registro.unpack(data, self.format, self.num_campos)

    def _append_record(self, path: str, rec: Registro) -> int:
        with open(path, 'ab') as f:
            f.write(rec.pack(self.format))
        total = os.path.getsize(path) - HEADER_SIZE
        return total // self._record_size() - 1

    def _count_records(self, path: str) -> int:
        return (os.path.getsize(path) - HEADER_SIZE) // self._record_size()

    def load_csv(self, csv_path: str) -> None:
        rows = list(csv.reader(open(csv_path, newline='', encoding='latin1')))
        self.num_campos = len(rows[0])
        self.format = self._gen_fmt(self.num_campos)

        # reinicia archivos (8 bytes cabecera en ambos)
        with open(self.data_file, 'wb') as f:
            f.write(struct.pack('ii', -1, self.num_campos))
        with open(self.aux_file, 'wb') as f:
            f.write(struct.pack('ii', -1, self.num_campos))

        # cargar y ordenar
        regs = [Registro([str(v)[:40] for v in row]) for row in rows]
        regs.sort(key=lambda r: r.campos[self.field_index])
        # enlazar
        for i, r in enumerate(regs):
            r.next_pos = i+1 if i+1 < len(regs) else -1

        with open(self.data_file, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('i', 0))
            f.write(struct.pack('i', self.num_campos))
            for r in regs:
                f.write(r.pack(self.format))

    def insert(self, _: None, values: List[str]) -> None:
        # VALIDACIÓN: Verificar que el número de campos coincida
        if self.num_campos == 0:
            # Si no hay esquema definido, usar los valores proporcionados
            self.num_campos = len(values)
            self.format = self._gen_fmt(self.num_campos)
            # Recrear archivos con el nuevo esquema
            with open(self.data_file, 'wb') as f:
                f.write(struct.pack('ii', -1, self.num_campos))
            with open(self.aux_file, 'wb') as f:
                f.write(struct.pack('ii', -1, self.num_campos))
        
        # Ajustar los valores al número de campos esperado
        adjusted_values = values[:self.num_campos]  # Truncar si hay más
        while len(adjusted_values) < self.num_campos:  # Rellenar si hay menos
            adjusted_values.append("")
        
        # Truncar cada campo a 40 caracteres máximo
        adjusted_values = [str(v)[:40] for v in adjusted_values]
        
        rec = Registro(adjusted_values)
        head = self._read_header(self.data_file)

        # Si está vacío, inserta como primer registro
        if head == -1:
            pos = self._append_record(self.data_file, rec)
            self._write_header(self.data_file, pos)
            self._write_numcampos(self.data_file)
            return

        # 1) Buscar el punto de inserción en la lista enlazada
        prev, cur = None, head
        while cur != -1:
            # Determinar si cur apunta a data_file o aux_file
            count_data = self._count_records(self.data_file)
            is_data    = cur < count_data
            path       = self.data_file if is_data else self.aux_file
            idx        = cur if is_data else cur - count_data

            node = self._read_record(path, idx)
            if node.campos[self.field_index] > rec.campos[self.field_index]:
                break
            prev, cur = cur, node.next_pos

        # Prepara el next_pos del nuevo registro
        rec.next_pos = cur

        # 2) Si el auxiliar está lleno, rebuild y reintentar
        if self._count_records(self.aux_file) >= MAX_AUX:
            self.rebuild()
            return self.insert(None, adjusted_values)

        # 3) Insertar en auxiliar y calcular posición absoluta
        count_data = self._count_records(self.data_file)
        aux_idx    = self._append_record(self.aux_file, rec)
        pos_aux    = count_data + aux_idx

        # 4) Ajustar punteros: header o el next_pos del anterior
        if prev is None:
            # Nuevo head
            self._write_header(self.data_file, pos_aux)
        else:
            # Leer el nodo previo y actualizar su next_pos
            count_data = self._count_records(self.data_file)
            prev_is    = prev < count_data
            ppath      = self.data_file if prev_is else self.aux_file
            pidx       = prev if prev_is else prev - count_data

            prec = self._read_record(ppath, pidx)
            prec.next_pos = pos_aux
            with open(ppath, 'r+b') as f:
                f.seek(HEADER_SIZE + pidx * self._record_size())
                f.write(prec.pack(self.format))

        # 5) Actualizar número de campos en el header
        self._write_numcampos(self.data_file)

    def scan_all(self) -> List[str]:
        out = []
        pos = self._read_header(self.data_file)
        while pos != -1:
            is_data = pos < self._count_records(self.data_file)
            path = self.data_file if is_data else self.aux_file
            idx = pos if is_data else pos - self._count_records(self.data_file)
            rec = self._read_record(path, idx)
            out.append(str(rec))
            pos = rec.next_pos
        return out

    def search(self, key: str, column: Optional[int] = None) -> List[str]:
        col = self.field_index if column is None else column
        results: List[str] = []

        if col == self.field_index:
            # === lógica indexada original ===
            pos = self._read_header(self.data_file)
            while pos != -1:
                # ¿estamos en data o aux?
                count_data = self._count_records(self.data_file)
                is_data    = pos < count_data
                path       = self.data_file if is_data else self.aux_file
                idx        = pos if is_data else pos - count_data

                rec = self._read_record(path, idx)
                val = rec.campos[self.field_index].strip()
                if val == key.strip():
                    results.append(str(rec))
                pos = rec.next_pos
            return results

        # === fallback full-scan ===
        for row in self.scan_all():
            cols = [c.strip() for c in row.split('|')]
            if col < len(cols) and cols[col] == key.strip():
                results.append(row)
        return results

    def range_search(self,
                     begin_key: str,
                     end_key:   str,
                     column:    Optional[int] = None
                    ) -> List[str]:
        col = self.field_index if column is None else column
        results: List[str] = []

        if col == self.field_index:
            # === lógica indexada original ===
            pos = self._read_header(self.data_file)
            while pos != -1:
                count_data = self._count_records(self.data_file)
                is_data    = pos < count_data
                path       = self.data_file if is_data else self.aux_file
                idx        = pos if is_data else pos - count_data

                rec = self._read_record(path, idx)
                val = rec.campos[self.field_index].strip()
                if begin_key <= val <= end_key:
                    results.append(str(rec))
                pos = rec.next_pos
            return results

        # === fallback full-scan ===
        for row in self.scan_all():
            cols = [c.strip() for c in row.split('|')]
            if col < len(cols) and begin_key <= cols[col] <= end_key:
                results.append(row)
        return results

    def remove(self, key: str) -> List[str]:
        removed: List[str] = []
        prev, cur = None, self._read_header(self.data_file)
        while cur != -1:
            is_data = cur < self._count_records(self.data_file)
            path = self.data_file if is_data else self.aux_file
            idx = cur if is_data else cur - self._count_records(self.data_file)
            rec = self._read_record(path, idx)
            if rec.campos[self.field_index] == key:
                removed.append(str(rec))
                # desenlaza
                if prev is None:
                    self._write_header(self.data_file, rec.next_pos)
                else:
                    prev_is = prev < self._count_records(self.data_file)
                    ppath = self.data_file if prev_is else self.aux_file
                    pidx = prev if prev_is else prev - self._count_records(self.data_file)
                    prec = self._read_record(ppath, pidx)
                    prec.next_pos = rec.next_pos
                    with open(ppath, 'r+b') as f:
                        f.seek(HEADER_SIZE + pidx * self._record_size())
                        f.write(prec.pack(self.format))
                break
            prev, cur = cur, rec.next_pos
        return removed

    def rebuild(self) -> None:
        # Merge main and aux files, sort, and rebuild the main file
        all_records = []
        pos = self._read_header(self.data_file)
        total_data = self._count_records(self.data_file)
        total_aux = self._count_records(self.aux_file)
        while pos != -1:
            is_data = pos < total_data
            path = self.data_file if is_data else self.aux_file
            idx = pos if is_data else pos - total_data
            rec = self._read_record(path, idx)
            all_records.append(rec)
            pos = rec.next_pos
        # Sort all records
        all_records.sort(key=lambda r: r.campos[self.field_index])
        # Relink
        for i, r in enumerate(all_records):
            r.next_pos = i+1 if i+1 < len(all_records) else -1
        # Rewrite data file
        with open(self.data_file, 'wb') as f:
            f.write(struct.pack('ii', 0 if all_records else -1, self.num_campos))
            for r in all_records:
                f.write(r.pack(self.format))
        # Reset aux file
        with open(self.aux_file, 'wb') as f:
            f.write(struct.pack('ii', -1, self.num_campos))