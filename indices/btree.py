import os
import pickle
from typing import Optional
from indices.base_index import BaseIndex

ORDER = 4

class BTreeNode:
    def __init__(self, is_leaf=True):
        self.keys = []  # lista de tuplas (key, value)
        self.children = []
        self.is_leaf = is_leaf
        self.next = None

class BPlusTree(BaseIndex):
    def __init__(self, path='btree_index.pkl'):
        self.path = path
        self.field_index = 0  # será sobrescrito por el Engine si es necesario
        if os.path.exists(self.path):
            with open(self.path, 'rb') as f:
                self.root = pickle.load(f)
        else:
            self.root = BTreeNode()
            self._save()

    def _save(self):
        with open(self.path, 'wb') as f:
            pickle.dump(self.root, f)

    def _parse_key(self, k):
        """Convierte la clave a int si es posible, si no la deja como string."""
        if isinstance(k, (int, float)):
            return k
        try:
            # Intentar convertir a int primero
            return int(k)
        except ValueError:
            try:
                # Si no es int, intentar float
                return float(k)
            except ValueError:
                # Si no es numérico, mantener como string pero limpiar espacios
                return str(k).strip()

    def _compare_keys(self, key1, key2):
        """Compara dos claves de manera segura, manejando tipos diferentes."""
        # Si ambas son del mismo tipo, comparar directamente
        if type(key1) == type(key2):
            return key1 < key2, key1 == key2, key1 > key2
        
        # Si una es numérica y otra string, convertir ambas a string para comparar
        str1 = str(key1)
        str2 = str(key2)
        return str1 < str2, str1 == str2, str1 > str2

    def search(self, key):
        key = self._parse_key(key)
        result = []
        
        # Buscar en todas las hojas para manejar duplicados correctamente
        node = self.root
        # Ir a la primera hoja
        while not node.is_leaf:
            node = node.children[0]
        
        # Recorrer todas las hojas
        while node:
            for k, v in node.keys:
                _, equals, _ = self._compare_keys(key, k)
                if equals:
                    result.append(f"{k} -> {v}")
            node = node.next
            
        return result

    def range_search(self, start_key, end_key):
        start_key = self._parse_key(start_key)
        end_key = self._parse_key(end_key)
        
        result = []
        node = self.root
        
        # Navegar hasta la hoja que contiene start_key
        while not node.is_leaf:
            i = 0
            while i < len(node.keys):
                _, _, greater = self._compare_keys(start_key, node.keys[i][0])
                if not greater:  # start_key <= node.keys[i][0]
                    break
                i += 1
            node = node.children[i]

        # Recorrer las hojas buscando claves en el rango
        while node:
            for k, v in node.keys:
                less_than_start, _, _ = self._compare_keys(k, start_key)
                _, _, greater_than_end = self._compare_keys(k, end_key)
                
                if not less_than_start and not greater_than_end:  # start_key <= k <= end_key
                    result.append(f"{k} -> {v}")
                elif greater_than_end:  # k > end_key
                    return result
            node = node.next
        return result

    def insert(self, _, values):
        # Usar self.field_index para determinar qué columna indexar
        if len(values) <= self.field_index:
            raise ValueError(f"El registro no tiene suficientes columnas. Se esperaba al menos {self.field_index + 1}, pero tiene {len(values)}")
        
        key = self._parse_key(values[self.field_index])
        
        # Crear el valor excluyendo la columna indexada - versión más simple
        value_parts = values[:self.field_index] + values[self.field_index+1:]
        value = ' | '.join(str(v) for v in value_parts)
        
        root = self.root
        if len(root.keys) == ORDER - 1:
            new_root = BTreeNode(is_leaf=False)
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self.root = new_root
        
        self._insert_non_full(self.root, key, value)
        self._save()

    def _insert_non_full(self, node, key, value):
        if node.is_leaf:
            node.keys.append((key, value))
            node.keys.sort(key=lambda kv: (str(type(kv[0]).__name__), kv[0]))  # Ordenar por tipo y luego por valor
        else:
            i = len(node.keys) - 1
            while i >= 0:
                less_than, _, _ = self._compare_keys(key, node.keys[i][0])
                if not less_than:  # key >= node.keys[i][0]
                    break
                i -= 1
            i += 1
            
            if i >= len(node.children):
                raise IndexError(f"Child index {i} out of range")
                
            if len(node.children[i].keys) == ORDER - 1:
                self._split_child(node, i)
                _, _, greater = self._compare_keys(key, node.keys[i][0])
                if greater:  # key > node.keys[i][0]
                    i += 1
            self._insert_non_full(node.children[i], key, value)

    def _split_child(self, parent, i):
        node = parent.children[i]
        mid = len(node.keys) // 2

        right = BTreeNode(is_leaf=node.is_leaf)
        
        if node.is_leaf:
            # Para hojas: el nodo derecho toma desde mid hasta el final
            right.keys = node.keys[mid:]
            node.keys = node.keys[:mid]
            # Mantener el enlace a la siguiente hoja
            right.next = node.next
            node.next = right
            # La clave que sube al padre es la primera clave del nodo derecho
            up_key = right.keys[0]
        else:
            # Para nodos internos: el nodo derecho toma desde mid+1 hasta el final
            right.keys = node.keys[mid+1:]
            right.children = node.children[mid+1:]
            # La clave que sube es la del medio
            up_key = node.keys[mid]
            # El nodo izquierdo mantiene solo hasta mid (excluyendo mid)
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]
        
        # Insertar la clave que sube y el nuevo hijo en el padre
        parent.keys.insert(i, up_key)
        parent.children.insert(i+1, right)

    def remove(self, key):
        """Elimina todos los registros con la clave especificada de todas las hojas."""
        key = self._parse_key(key)
        total_removed = 0
        
        # Ir a la primera hoja (más a la izquierda)
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        
        # Recorrer todas las hojas usando los enlaces next
        while node:
            # Contar cuántos registros había antes
            original_count = len(node.keys)
            
            # Filtrar las claves que NO coinciden (mantener las que son diferentes)
            node.keys = [(k, v) for (k, v) in node.keys 
                         if not self._compare_keys(key, k)[1]]  # No son iguales
            
            # Contar cuántos se eliminaron de esta hoja
            removed_from_this_node = original_count - len(node.keys)
            total_removed += removed_from_this_node
            
            # Ir a la siguiente hoja
            node = node.next
        
        # Guardar los cambios
        self._save()
        return [f"Se eliminaron {total_removed} registros con clave '{key}'"]

    def scan_all(self):
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        result = []
        while node:
            for k, v in node.keys:
                result.append(f"{k} -> {v}")
            node = node.next
        return result

    def load_csv(self, path_or_data, index_col: Optional[int] = None):
        """Carga un CSV usando la columna especificada como índice."""
        import csv
        
        # Si recibe una lista de diccionarios (para compatibilidad con ISAM)
        if isinstance(path_or_data, list):
            for data_dict in path_or_data:
                values = list(data_dict.values())
                self.insert(None, values)
            return
        
        # Si recibe un path (string)
        path = path_or_data
        if index_col is None:
            index_col = self.field_index
            
        with open(path, newline='', encoding='latin1') as f:
            reader = csv.reader(f)
            headers = next(reader)  # Saltar cabeceras
            
            if index_col >= len(headers):
                raise ValueError(f"La columna {index_col} no existe. El CSV solo tiene {len(headers)} columnas (0-{len(headers)-1})")
            
            for row in reader:
                if len(row) < len(headers):
                    # Rellenar con valores vacíos si faltan columnas
                    while len(row) < len(headers):
                        row.append("")
                
                if len(row) > index_col:
                    self.insert(None, row)