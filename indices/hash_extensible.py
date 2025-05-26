# indices/hash_extensible.py - VERSIÓN COMPLETAMENTE CORREGIDA
import os
import struct
import pickle
import hashlib
import csv
from typing import Dict, List, Optional, Any
from indices.base_index import BaseIndex

BUCKET_CAPACITY = 32
MAX_GLOBAL_DEPTH = 8

class Bucket:
    def __init__(self, local_depth: int):
        self.records: List[List[str]] = []  # CORREGIDO: Guardar como listas de strings
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
        self.field_index: int = 0  # NUEVO: Campo indexado
        
        # Crear directorios necesarios
        os.makedirs(os.path.dirname(dir_file), exist_ok=True)
        os.makedirs("indices/buckets", exist_ok=True)
        
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
        bucket_id = len([f for f in os.listdir("indices/buckets") if f.startswith("bucket_")]) if os.path.exists("indices/buckets") else 0
        bucket_path = f"indices/buckets/bucket_{bucket_id}_{local_depth}.pkl"
        
        with open(bucket_path, 'wb') as f:
            pickle.dump(Bucket(local_depth), f)
        return bucket_path

    def _load_directory(self) -> None:
        try:
            with open(self.dir_file, 'rb') as f:
                data = pickle.load(f)
                self.global_depth = data.get('global_depth', 1)
                self.directory = data.get('directory', {})
                self.field_index = data.get('field_index', 0)
        except Exception as e:
            print(f"Error cargando directorio hash: {e}")
            self._create_new_directory()

    def _save_directory(self) -> None:
        try:
            with open(self.dir_file, 'wb') as f:
                pickle.dump({
                    'global_depth': self.global_depth,
                    'directory': self.directory,
                    'field_index': self.field_index
                }, f)
        except Exception as e:
            print(f"Error guardando directorio hash: {e}")

    def _hash(self, key: str) -> str:
        """Genera hash binario de una clave"""
        hash_obj = hashlib.sha256(key.encode('utf-8'))
        hash_int = int(hash_obj.hexdigest(), 16)
        binary = bin(hash_int)[2:].zfill(256)
        return binary[:self.global_depth]

    def _find_bucket_path(self, hash_key: str) -> str:
        """Encuentra el bucket correcto para una clave hash"""
        # Buscar desde la profundidad actual hacia abajo
        for depth in range(min(len(hash_key), self.global_depth), 0, -1):
            prefix = hash_key[:depth]
            if prefix in self.directory:
                return self.directory[prefix]
        
        # Fallback: usar el primer bucket disponible
        if self.directory:
            return list(self.directory.values())[0]
        else:
            # Crear bucket por defecto si no existe
            default_path = self._create_bucket(1)
            self.directory['0'] = default_path
            self._save_directory()
            return default_path

    def _load_bucket(self, path: str) -> Bucket:
        """Carga un bucket desde archivo"""
        try:
            if os.path.exists(path):
                with open(path, 'rb') as f:
                    return pickle.load(f)
            else:
                # Crear bucket nuevo si no existe
                bucket = Bucket(1)
                self._save_bucket(path, bucket)
                return bucket
        except Exception as e:
            print(f"Error cargando bucket {path}: {e}")
            return Bucket(1)

    def _save_bucket(self, path: str, bucket: Bucket) -> None:
        """Guarda un bucket en archivo"""
        try:
            with open(path, 'wb') as f:
                pickle.dump(bucket, f)
        except Exception as e:
            print(f"Error guardando bucket {path}: {e}")

    def insert(self, _, values: List[str]) -> None:
        """Insertar un registro en el hash"""
        if not values:
            return
        
        # Usar el campo indexado como clave
        if self.field_index < len(values):
            key = str(values[self.field_index]).strip()
        else:
            key = str(values[0]).strip()  # Fallback al primer campo
        
        # Generar hash de la clave
        hash_key = self._hash(key)
        bucket_path = self._find_bucket_path(hash_key)
        bucket = self._load_bucket(bucket_path)

        # Insertar en bucket o manejar overflow
        if not bucket.is_full():
            bucket.records.append(values)
            self._save_bucket(bucket_path, bucket)
        else:
            self._handle_full_bucket(bucket_path, bucket, hash_key, values)

    def _handle_full_bucket(self, bucket_path: str, bucket: Bucket, hash_key: str, values: List[str]) -> None:
        """Maneja buckets llenos: split o overflow"""
        if bucket.local_depth < MAX_GLOBAL_DEPTH and bucket.local_depth < len(hash_key):
            self._split_bucket(bucket_path, bucket, hash_key)
            # Reintentar inserción después del split
            self.insert(None, values)
        else:
            # Crear bucket de overflow
            self._add_overflow(bucket_path, bucket, values)

    def _split_bucket(self, old_path: str, old_bucket: Bucket, hash_key: str) -> None:
        """Divide un bucket en dos cuando está lleno"""
        new_depth = old_bucket.local_depth + 1
        new_bucket0 = Bucket(new_depth)
        new_bucket1 = Bucket(new_depth)
        
        # Redistribuir registros basado en el bit adicional
        for record in old_bucket.records:
            if self.field_index < len(record):
                rec_key = str(record[self.field_index]).strip()
            else:
                rec_key = str(record[0]).strip()
            
            rec_hash = self._hash(rec_key)
            
            # Usar el bit en la posición new_depth-1 para decidir bucket
            if len(rec_hash) >= new_depth and rec_hash[new_depth - 1] == '1':
                new_bucket1.records.append(record)
            else:
                new_bucket0.records.append(record)

        # Crear nuevos paths
        new_path0 = self._create_bucket(new_depth)
        new_path1 = self._create_bucket(new_depth)
        
        # Actualizar directorio
        base_prefix = hash_key[:old_bucket.local_depth]
        new_prefix0 = base_prefix + '0'
        new_prefix1 = base_prefix + '1'
        
        # Remover referencia al bucket viejo
        keys_to_remove = [k for k, v in self.directory.items() if v == old_path]
        for k in keys_to_remove:
            del self.directory[k]
        
        # Agregar nuevas referencias
        self.directory[new_prefix0] = new_path0
        self.directory[new_prefix1] = new_path1
        
        # Actualizar profundidad global si es necesario
        if new_depth > self.global_depth:
            self.global_depth = new_depth
        
        # Guardar nuevos buckets
        self._save_bucket(new_path0, new_bucket0)
        self._save_bucket(new_path1, new_bucket1)
        
        # Remover bucket viejo
        try:
            if os.path.exists(old_path):
                os.remove(old_path)
        except Exception as e:
            print(f"Error removiendo bucket viejo {old_path}: {e}")
        
        self._save_directory()

    def _add_overflow(self, parent_path: str, parent_bucket: Bucket, values: List[str]) -> None:
        """Agrega bucket de overflow cuando no se puede hacer split"""
        # Buscar el último bucket en la cadena de overflow
        current_path = parent_path
        current_bucket = parent_bucket
        
        while current_bucket.next_bucket:
            current_path = current_bucket.next_bucket
            current_bucket = self._load_bucket(current_path)
        
        # Crear nuevo bucket de overflow
        overflow_id = len([f for f in os.listdir("indices/buckets") if f.startswith("overflow_")]) if os.path.exists("indices/buckets") else 0
        overflow_path = f"indices/buckets/overflow_{overflow_id}.pkl"
        overflow_bucket = Bucket(current_bucket.local_depth)
        overflow_bucket.records.append(values)
        
        # Enlazar buckets
        current_bucket.next_bucket = overflow_path
        self._save_bucket(current_path, current_bucket)
        self._save_bucket(overflow_path, overflow_bucket)

    def search(self, key: str) -> List[List[str]]:
        """Busca registros por clave - RETORNA LISTAS"""
        key = str(key).strip()
        hash_key = self._hash(key)
        bucket_path = self._find_bucket_path(hash_key)
        
        results = []
        visited_buckets = set()
        
        # Buscar en bucket principal y cadena de overflow
        current_path = bucket_path
        while current_path and current_path not in visited_buckets:
            visited_buckets.add(current_path)
            bucket = self._load_bucket(current_path)
            
            # Buscar en registros del bucket
            for record in bucket.records:
                if self.field_index < len(record):
                    record_key = str(record[self.field_index]).strip()
                else:
                    record_key = str(record[0]).strip()
                
                if record_key == key:
                    results.append(record)
            
            # Seguir cadena de overflow
            current_path = bucket.next_bucket
        
        return results

    def remove(self, key: str) -> List[str]:
        """Elimina registros por clave - RETORNA STRINGS CSV"""
        key = str(key).strip()
        hash_key = self._hash(key)
        bucket_path = self._find_bucket_path(hash_key)
        
        removed_records = []
        visited_buckets = set()
        
        # Buscar y eliminar en bucket principal y cadena de overflow
        current_path = bucket_path
        while current_path and current_path not in visited_buckets:
            visited_buckets.add(current_path)
            bucket = self._load_bucket(current_path)
            
            # Buscar registros a eliminar
            records_to_remove = []
            for i, record in enumerate(bucket.records):
                if self.field_index < len(record):
                    record_key = str(record[self.field_index]).strip()
                else:
                    record_key = str(record[0]).strip()
                
                if record_key == key:
                    records_to_remove.append(i)
                    # Formatear como CSV
                    csv_record = ','.join(str(v).strip() for v in record)
                    removed_records.append(csv_record)
            
            # Remover registros (en orden inverso para mantener índices)
            for i in reversed(records_to_remove):
                del bucket.records[i]
            
            # Guardar bucket modificado
            if records_to_remove:
                self._save_bucket(current_path, bucket)
            
            # Seguir cadena de overflow
            current_path = bucket.next_bucket
        
        return removed_records

    def scan_all(self) -> List[List[str]]:
        """Escanea todos los registros - RETORNA LISTAS"""
        all_records = []
        visited_buckets = set()
        
        # Recorrer todos los buckets únicos
        for bucket_path in set(self.directory.values()):
            if bucket_path in visited_buckets:
                continue
            
            # Recorrer bucket y su cadena de overflow
            current_path = bucket_path
            while current_path and current_path not in visited_buckets:
                visited_buckets.add(current_path)
                bucket = self._load_bucket(current_path)
                
                # Agregar todos los registros del bucket
                for record in bucket.records:
                    all_records.append(record)
                
                # Seguir cadena de overflow
                current_path = bucket.next_bucket
        
        return all_records

    def load_csv(self, csv_path: str, index_col: int = 0) -> None:
        """Carga datos desde un archivo CSV"""
        self.field_index = index_col
        
        try:
            with open(csv_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f)
                headers = next(reader, [])  # Skip header
                
                for row_num, row in enumerate(reader):
                    if not row or len(row) == 0:
                        continue
                    
                    # Limpiar y preparar datos
                    clean_row = [str(cell).strip() for cell in row]
                    
                    # Asegurar que tiene al menos una columna para indexar
                    if len(clean_row) > index_col:
                        self.insert(None, clean_row)
                    else:
                        print(f"Fila {row_num + 1} omitida: no tiene suficientes columnas")
                
                print(f"Hash Extensible: Cargados datos desde {csv_path} indexando por columna {index_col}")
                
        except Exception as e:
            print(f"Error cargando CSV en Hash: {e}")
            raise

    def range_search(self, begin_key: str, end_key: str) -> List[str]:
        """Hash NO soporta búsquedas por rango eficientemente"""
        raise NotImplementedError("Hash Extensible no soporta búsquedas por rango. Use ISAM o B+ Tree para rangos.")

    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del hash"""
        total_records = 0
        total_buckets = len(set(self.directory.values()))
        overflow_buckets = 0
        
        visited = set()
        for bucket_path in set(self.directory.values()):
            if bucket_path in visited:
                continue
            visited.add(bucket_path)
            
            current_path = bucket_path
            while current_path:
                bucket = self._load_bucket(current_path)
                total_records += len(bucket.records)
                
                if bucket.next_bucket:
                    overflow_buckets += 1
                
                current_path = bucket.next_bucket
                if current_path:
                    visited.add(current_path)
        
        return {
            'global_depth': self.global_depth,
            'total_buckets': total_buckets,
            'overflow_buckets': overflow_buckets,
            'total_records': total_records,
            'directory_size': len(self.directory),
            'field_index': self.field_index
        }