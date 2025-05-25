# indices/rtree.py

from rtree.index import Index, Property
import math
import csv
from indices.base_index import BaseIndex

class MultidimensionalRTree(BaseIndex):
    def __init__(self, path='rtree_vectors', dimension=2):
        p = Property()
        p.dimension = dimension
        p.dat_extension = 'data'
        p.idx_extension = 'index'
        self.idx = Index(path, properties=p)
        self.dimension = dimension
        self.id_counter = 0
        self.data_map = {}    # ID → objeto original
        self.vector_map = {}  # ID → vector original

    def insert(self, vector, obj):
        if len(vector) != self.dimension:
            raise ValueError("Dimensión incorrecta del vector")
        rect = tuple(vector + vector)  # bounding box (min...max)
        self.idx.insert(self.id_counter, rect)
        self.data_map[self.id_counter] = obj
        self.vector_map[self.id_counter] = vector
        self.id_counter += 1

    def range_search(self, point, param):
        if isinstance(param, int):
            return self._knn_search(point, param)
        else:
            return self._radius_search(point, param)

    def _radius_search(self, query, radius):
        if len(query) != self.dimension:
            raise ValueError("Dimensión incorrecta")
        rect = tuple([q - radius for q in query] + [q + radius for q in query])
        result = []
        for r in self.idx.intersection(rect, objects=True):
            vector = self.vector_map[r.id]
            dist = self._euclidean(query, vector)
            if dist <= radius:
                result.append((dist, self.data_map[r.id]))
        result.sort(key=lambda x: x[0])
        return result

    def _knn_search(self, query, k):
        if len(query) != self.dimension:
            raise ValueError("Dimensión incorrecta")
        rect = tuple(query + query)
        result = []
        for r in self.idx.nearest(rect, k, objects=True):
            vector = self.vector_map[r.id]
            dist = self._euclidean(query, vector)
            result.append((dist, self.data_map[r.id]))
        return result

    def _euclidean(self, a, b):
        return math.sqrt(sum((ai - bi) ** 2 for ai, bi in zip(a, b)))

    def scan_all(self):
        return [(v, self.data_map[i]) for i, v in self.vector_map.items()]

    # -----------------------------
    # Métodos adicionales requeridos
    # -----------------------------

    def insert_record(self, record):
        """
        Inserta un registro en el índice.
        Asume que `record` es una lista de floats que representan el vector completo,
        y guarda el propio `record` como objeto asociado.
        """
        vector = [float(x) for x in record]
        self.insert(vector, record)

    def search(self, key):
        """
        Busca un objeto por su ID en el índice.
        `key` debe ser convertible a entero.
        Devuelve lista con el objeto si existe, o lista vacía.
        """
        idx = int(key)
        obj = self.data_map.get(idx)
        return [obj] if obj is not None else []

    def remove(self, key):
        """
        Elimina la entrada con el ID dado del índice.
        `key` debe ser convertible a entero.
        """
        idx = int(key)
        vector = self.vector_map.get(idx)
        if vector is not None:
            rect = tuple(vector + vector)
            self.idx.delete(idx, rect)
            del self.vector_map[idx]
            del self.data_map[idx]

    def load_csv(self, path, delimiter=','):
        """
        Carga registros desde un CSV con encabezado, 
        extrae latitud (columna 4) y longitud (columna 5),
        e inserta cada fila en el índice.
        """
        with open(path, newline='', encoding='latin1') as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader, None)  # saltar cabecera
            for row in reader:
                # asegurarse de que haya al menos 6 columnas
                if len(row) < 6:
                    continue
                try:
                    lat = float(row[4])
                    lon = float(row[5])
                except ValueError:
                    continue  # fila sin coordenadas válidas
                # insertar vector [lat, lon] y guardar la fila completa como objeto
                self.insert([lat, lon], row)
