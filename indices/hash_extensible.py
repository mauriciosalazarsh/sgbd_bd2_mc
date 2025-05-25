# indices/hash_extensible.py
import os
import struct
import pickle
import hashlib
from typing import Dict, List, Optional
from indices.base_index import BaseIndex

BUCKET_CAPACITY = 32
MAX_GLOBAL_DEPTH = 8
RECORD_FORMAT = '20s20s20sii'
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

class Registro:
    def __init__(self, clave: bytes = b'', campo1: bytes = b'', campo2: bytes = b'', campo3: bytes = b'', pos: int = 0):
        self.clave = clave.ljust(20)[:20]
        self.campo1 = campo1.ljust(20)[:20]
        self.campo2 = campo2.ljust(20)[:20]
        self.campo3 = campo3.ljust(20)[:20]
        self.pos = pos

    def pack(self) -> bytes:
        return struct.pack(RECORD_FORMAT, self.clave, self.campo1, self.campo2, self.campo3, self.pos)

    @classmethod
    def unpack(cls, data: bytes) -> 'Registro':
        return cls(*struct.unpack(RECORD_FORMAT, data))

class Bucket:
    def __init__(self, local_depth: int):
        self.records: List[Registro] = []
        self.local_depth = local_depth
        self.next_bucket: Optional[str] = None  # Path para overflow

    def is_full(self) -> bool:
        return len(self.records) >= BUCKET_CAPACITY

class ExtendibleHash(BaseIndex):
    def __init__(self, dir_file: str = 'hash_dir.pkl', data_file: str = 'hash_data.bin'):
        self.dir_file = dir_file
        self.data_file = data_file
        self.global_depth: int = 1
        self.directory: Dict[str, str] = {}  # {hash_prefix: bucket_path}
        self._init_directory()

    def _init_directory(self) -> None:
        if os.path.exists(self.dir_file):
            self._load_directory()
        else:
            self._create_new_directory()

    def _create_new_directory(self) -> None:
        self.directory = {
            '0': self._create_bucket(1),
            '1': self._create_bucket(1)
        }
        self._save_directory()

    def _create_bucket(self, local_depth: int) -> str:
        bucket_path = f"indices/buckets/bucket_{len(self.directory)}.bin"
        os.makedirs(os.path.dirname(bucket_path), exist_ok=True)
        with open(bucket_path, 'wb') as f:
            pickle.dump(Bucket(local_depth), f)
        return bucket_path

    def _load_directory(self) -> None:
        with open(self.dir_file, 'rb') as f:
            data = pickle.load(f)
            self.global_depth = data['global_depth']
            self.directory = data['directory']

    def _save_directory(self) -> None:
        with open(self.dir_file, 'wb') as f:
            pickle.dump({
                'global_depth': self.global_depth,
                'directory': self.directory
            }, f)

    def _hash(self, key: str) -> str:
        return bin(int(hashlib.sha256(key.encode()).hexdigest(), 16))[2:].zfill(256)[:self.global_depth]

    def insert(self, _, values: List[str]) -> None:
        key = values[0]
        campos = values[1:4] + [''] * (4 - len(values))
        registro = Registro(
            key.encode('utf-8'),
            campos[0].encode('utf-8'),
            campos[1].encode('utf-8'),
            campos[2].encode('utf-8')
        )
        
        hash_key = self._hash(key)
        bucket_path = self._find_bucket_path(hash_key)
        bucket = self._load_bucket(bucket_path)

        if not bucket.is_full():
            bucket.records.append(registro)
            self._save_bucket(bucket_path, bucket)
        else:
            self._handle_full_bucket(bucket_path, bucket, hash_key, registro)

    def _find_bucket_path(self, hash_key: str) -> str:
        for depth in range(self.global_depth, 0, -1):
            prefix = hash_key[:depth]
            if prefix in self.directory:
                return self.directory[prefix]
        return self.directory['0']

    def _load_bucket(self, path: str) -> Bucket:
        with open(path, 'rb') as f:
            return pickle.load(f)

    def _save_bucket(self, path: str, bucket: Bucket) -> None:
        with open(path, 'wb') as f:
            pickle.dump(bucket, f)

    def _handle_full_bucket(self, bucket_path: str, bucket: Bucket, hash_key: str, registro: Registro) -> None:
        if bucket.local_depth < MAX_GLOBAL_DEPTH:
            self._split_bucket(bucket_path, bucket, hash_key)
            self.insert(None, [registro.clave.decode().strip()] + [
                registro.campo1.decode().strip(),
                registro.campo2.decode().strip(),
                registro.campo3.decode().strip()
            ])
        else:
            self._add_overflow(bucket_path, bucket, registro)

    def _split_bucket(self, old_path: str, old_bucket: Bucket, hash_key: str) -> None:
        new_depth = old_bucket.local_depth + 1
        new_bucket0 = Bucket(new_depth)
        new_bucket1 = Bucket(new_depth)
        
        mask = 1 << (new_depth - 1)
        for reg in old_bucket.records:
            h = int.from_bytes(reg.clave.strip(), byteorder='big')
            if h & mask:
                new_bucket1.records.append(reg)
            else:
                new_bucket0.records.append(reg)

        base_prefix = hash_key[:old_bucket.local_depth]
        new_path0 = self._create_bucket(new_depth)
        new_path1 = self._create_bucket(new_depth)
        
        # Actualizar directorio
        new_prefix0 = base_prefix + '0'
        new_prefix1 = base_prefix + '1'
        self.directory = {k: v for k, v in self.directory.items() if v != old_path}
        self.directory[new_prefix0] = new_path0
        self.directory[new_prefix1] = new_path1
        
        if new_depth > self.global_depth:
            self.global_depth = new_depth
            
        self._save_bucket(new_path0, new_bucket0)
        self._save_bucket(new_path1, new_bucket1)
        os.remove(old_path)
        self._save_directory()

    def _add_overflow(self, parent_path: str, parent_bucket: Bucket, registro: Registro) -> None:
        current = parent_bucket
        while current.next_bucket:
            current = self._load_bucket(current.next_bucket)
        
        overflow_path = f"indices/buckets/overflow_{len(self.directory)}.bin"
        overflow_bucket = Bucket(parent_bucket.local_depth)
        overflow_bucket.records.append(registro)
        
        current.next_bucket = overflow_path
        self._save_bucket(parent_path, parent_bucket)
        self._save_bucket(overflow_path, overflow_bucket)
        self.directory[overflow_path] = overflow_path
        self._save_directory()

    def search(self, key: str) -> List[Registro]:
        hash_key = self._hash(key)
        bucket_path = self._find_bucket_path(hash_key)
        bucket = self._load_bucket(bucket_path)
        
        results = []
        current = bucket
        while current:
            for reg in current.records:
                if reg.clave.decode().strip() == key:
                    results.append(reg)
            current = self._load_bucket(current.next_bucket) if current.next_bucket else None
        return results

    def remove(self, key: str) -> None:
        hash_key = self._hash(key)
        bucket_path = self._find_bucket_path(hash_key)
        bucket = self._load_bucket(bucket_path)
        
        current = bucket
        prev = None
        while current:
            found = False
            for i, reg in enumerate(current.records):
                if reg.clave.decode().strip() == key:
                    del current.records[i]
                    found = True
                    break
            if found:
                self._save_bucket(bucket_path, current)
                break
            prev = current
            current = self._load_bucket(current.next_bucket) if current.next_bucket else None

    def scan_all(self) -> List[str]:
        seen = set()
        results = []
        for path in set(self.directory.values()):
            if path not in seen:
                seen.add(path)
                current = self._load_bucket(path)
                while current:
                    for reg in current.records:
                        record_str = " | ".join([
                            reg.clave.decode().strip(),
                            reg.campo1.decode().strip(),
                            reg.campo2.decode().strip(),
                            reg.campo3.decode().strip()
                        ])
                        results.append(record_str)
                    current = self._load_bucket(current.next_bucket) if current.next_bucket else None
        return results

    def load_csv(self, csv_path: str, index_col: int = 0) -> None:
        import csv
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if len(row) <= index_col:
                    continue
                key = row[index_col].strip()
                fields = [field.strip() for i, field in enumerate(row) if i != index_col][:3]
                fields += [''] * (3 - len(fields))
                self.insert(None, [key] + fields)