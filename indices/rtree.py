# indices/rtree.py - ARCHIVO COMPLETO CON TODAS LAS CORRECCIONES

from rtree.index import Index, Property
import math
import csv
import os
import pickle
from indices.base_index import BaseIndex

class MultidimensionalRTree(BaseIndex):
    def __init__(self, path='rtree_vectors', dimension=2):
        self.dimension = dimension
        self.id_counter = 0
        self.data_map = {}    # ID → objeto original
        self.vector_map = {}  # ID → vector original
        
        # Crear directorio para archivos si no existe
        os.makedirs('embeddings', exist_ok=True)
        
        # Ruta completa para archivos del R-Tree
        self.rtree_path = os.path.join('embeddings', path)
        
        try:
            # Configurar propiedades del R-Tree para MEMORIA SECUNDARIA
            p = Property()
            p.dimension = dimension
            p.dat_extension = 'data'  # Archivo .data
            p.idx_extension = 'index'  # Archivo .index
            p.pagesize = 4096  # Tamaño de página en bytes (memoria secundaria)
            p.leaf_capacity = 100  # Capacidad de nodos hoja
            p.index_capacity = 100  # Capacidad de nodos internos
            
            # Limpiar archivos existentes si hay problemas
            self._clean_existing_files()
            
            # Crear índice R-Tree en archivos
            self.idx = Index(self.rtree_path, properties=p)
            
            print(f" R-Tree creado en archivos: {self.rtree_path}.data, {self.rtree_path}.index")
            
        except Exception as e:
            print(f"Error creando R-Tree: {e}")
            # Intentar con configuración más básica
            try:
                p = Property()
                p.dimension = dimension
                self.idx = Index(self.rtree_path, properties=p)
                print(f" R-Tree creado con configuración básica")
            except Exception as e2:
                raise Exception(f"No se pudo crear R-Tree: {e2}")

    def _clean_existing_files(self):
        """Limpiar archivos del R-Tree si existen y están corruptos"""
        try:
            for ext in ['.data', '.index', '.dat', '.idx']:
                file_path = self.rtree_path + ext
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        print(f"Archivo {file_path} eliminado para recrear")
                    except:
                        pass
        except:
            pass

    def insert(self, vector, obj):
        """Insertar punto espacial en memoria secundaria"""
        try:
            if len(vector) != self.dimension:
                raise ValueError("Dimensión incorrecta del vector")
            
            # Validar coordenadas
            clean_vector = []
            for coord in vector:
                try:
                    clean_coord = float(coord)
                    if math.isnan(clean_coord) or math.isinf(clean_coord):
                        return  # Saltar coordenadas inválidas
                    clean_vector.append(clean_coord)
                except (ValueError, TypeError):
                    return
            
            # Crear bounding box
            rect = tuple(clean_vector + clean_vector)
            
            # Insertar en R-Tree (se guarda en archivo)
            self.idx.insert(self.id_counter, rect)
            
            # Mapas auxiliares también en memoria secundaria via pickle
            self.data_map[self.id_counter] = obj
            self.vector_map[self.id_counter] = clean_vector
            self.id_counter += 1
            
            # Sincronizar cambios al disco cada cierto número de inserciones
            if self.id_counter % 1000 == 0:
                self._sync_to_disk()
                
        except Exception as e:
            print(f"Error insertando en R-Tree: {e}")

    def _sync_to_disk(self):
        """Forzar escritura a disco para persistencia"""
        try:
            # Forzar flush del R-Tree
            if hasattr(self.idx, 'flush'):
                self.idx.flush()
            
            # Guardar mapas auxiliares en archivos pickle
            with open(f"{self.rtree_path}_data_map.pkl", 'wb') as f:
                pickle.dump(self.data_map, f)
            
            with open(f"{self.rtree_path}_vector_map.pkl", 'wb') as f:
                pickle.dump(self.vector_map, f)
                
            with open(f"{self.rtree_path}_counter.pkl", 'wb') as f:
                pickle.dump(self.id_counter, f)
                
        except Exception as e:
            print(f"Warning: No se pudo sincronizar a disco: {e}")

    def _load_from_disk(self):
        """Cargar mapas auxiliares desde disco si existen"""
        try:
            # Cargar data_map
            data_map_file = f"{self.rtree_path}_data_map.pkl"
            if os.path.exists(data_map_file):
                with open(data_map_file, 'rb') as f:
                    self.data_map = pickle.load(f)
            
            # Cargar vector_map
            vector_map_file = f"{self.rtree_path}_vector_map.pkl"
            if os.path.exists(vector_map_file):
                with open(vector_map_file, 'rb') as f:
                    self.vector_map = pickle.load(f)
            
            # Cargar counter
            counter_file = f"{self.rtree_path}_counter.pkl"
            if os.path.exists(counter_file):
                with open(counter_file, 'rb') as f:
                    self.id_counter = pickle.load(f)
                    
            print(f" Datos cargados desde disco: {len(self.data_map)} registros")
            
        except Exception as e:
            print(f"Info: No se pudieron cargar datos previos: {e}")

    def range_search(self, point, param):
        """Búsqueda espacial usando archivos con distancia haversine"""
        try:
            if len(point) != self.dimension:
                return []
            
            # Limpiar coordenadas
            clean_point = []
            for coord in point:
                try:
                    clean_coord = float(coord)
                    if math.isnan(clean_coord) or math.isinf(clean_coord):
                        return []
                    clean_point.append(clean_coord)
                except (ValueError, TypeError):
                    return []
            
            # Determinar tipo de búsqueda
            if isinstance(param, int) or (isinstance(param, str) and param.isdigit()):
                return self._knn_search(clean_point, int(param))
            else:
                return self._radius_search(clean_point, float(param))
                
        except Exception as e:
            print(f"Error en range_search: {e}")
            return []

    def _radius_search(self, query, radius_km):
        """Búsqueda por radio usando distancia haversine real en km"""
        try:
            if len(query) != self.dimension or radius_km <= 0:
                return []
            
            # Convertir radio de km a grados aproximadamente
            # 1 grado ≈ 111 km en el ecuador
            lat = query[0]
            radius_deg = radius_km / 111.0
            
            # Ajustar por la latitud (los grados de longitud se acortan cerca de los polos)
            radius_lon_deg = radius_deg / max(math.cos(math.radians(lat)), 0.01)
            
            # Crear bounding box en grados
            min_lat = query[0] - radius_deg
            max_lat = query[0] + radius_deg
            min_lon = query[1] - radius_lon_deg  
            max_lon = query[1] + radius_lon_deg
            
            rect = (min_lat, min_lon, max_lat, max_lon)
            
            result = []
            # Buscar candidatos en el bounding box
            for r in self.idx.intersection(rect, objects=True):
                if r.id in self.vector_map and r.id in self.data_map:
                    vector = self.vector_map[r.id]
                    # Usar distancia haversine real
                    dist_km = self._haversine_distance(query, vector)
                    
                    # Filtro exacto por distancia en km
                    if dist_km <= radius_km:
                        result.append((dist_km, self.data_map[r.id]))
            
            result.sort(key=lambda x: x[0])
            return result
            
        except Exception as e:
            print(f"Error en _radius_search: {e}")
            return []

    def _knn_search(self, query, k):
        """Búsqueda KNN usando distancia haversine real en km"""
        try:
            if len(query) != self.dimension or k <= 0:
                return []
            
            k = min(k, len(self.data_map))
            if k == 0:
                return []
            
            # Para KNN, calcular distancias a todos los puntos
            all_distances = []
            for item_id in self.data_map:
                if item_id in self.vector_map:
                    vector = self.vector_map[item_id]
                    dist_km = self._haversine_distance(query, vector)
                    all_distances.append((dist_km, self.data_map[item_id]))
            
            # Ordenar por distancia y tomar los K primeros
            all_distances.sort(key=lambda x: x[0])
            return all_distances[:k]
            
        except Exception as e:
            print(f"Error en _knn_search: {e}")
            return []

    def _haversine_distance(self, coord1, coord2):
        """Distancia haversine entre dos coordenadas geográficas en km"""
        try:
            lat1, lon1 = math.radians(float(coord1[0])), math.radians(float(coord1[1]))
            lat2, lon2 = math.radians(float(coord2[0])), math.radians(float(coord2[1]))
            
            # Diferencias
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            
            # Fórmula haversine
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            
            # Radio de la Tierra en kilómetros
            earth_radius_km = 6371.0
            
            return earth_radius_km * c
            
        except (ValueError, TypeError, OverflowError):
            return float('inf')

    def _euclidean(self, a, b):
        """Usar distancia haversine en lugar de euclidiana para coordenadas geográficas"""
        return self._haversine_distance(a, b)

    def scan_all(self):
        """Escanear todos los datos desde archivos"""
        try:
            result = []
            for item_id in self.data_map:
                if item_id in self.vector_map:
                    vector = self.vector_map[item_id]
                    obj = self.data_map[item_id]
                    result.append((vector, obj))
            return result
        except:
            return []

    def load_csv(self, path, delimiter=','):
        """Cargar CSV y persistir en memoria secundaria con distancia haversine"""
        record_count = 0
        
        # Intentar cargar datos previos
        self._load_from_disk()
        
        try:
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.reader(f, delimiter=delimiter)
                header = next(reader, None)
                
                lat_col, lon_col = self._find_coordinate_columns(header)
                print(f" Usando columnas - Latitud: {lat_col}, Longitud: {lon_col}")
                
                for row in reader:
                    if len(row) <= max(lat_col, lon_col):
                        continue
                    
                    try:
                        lat_str = row[lat_col].strip()
                        lon_str = row[lon_col].strip()
                        
                        # Saltar filas vacías
                        if not lat_str or not lon_str:
                            continue
                        
                        lat = float(lat_str)
                        lon = float(lon_str)
                        
                        # Validar rango de coordenadas geográficas
                        if -90 <= lat <= 90 and -180 <= lon <= 180:
                            self.insert([lat, lon], row)
                            record_count += 1
                            
                    except (ValueError, IndexError):
                        continue
                
                # Sincronizar todos los cambios al disco
                self._sync_to_disk()
                print(f" R-Tree guardado en memoria secundaria: {record_count} registros")
                
        except Exception as e:
            print(f"Error cargando CSV: {e}")
        
        return record_count

    def _find_coordinate_columns(self, header):
        """Identificar columnas de coordenadas automáticamente"""
        if not header:
            return 4, 5  # Fallback por defecto
        
        lat_col = lon_col = None
        
        # Buscar por nombre de columna
        for i, col_name in enumerate(header):
            col_lower = col_name.lower().strip()
            if any(keyword in col_lower for keyword in ['lat', 'latitude']):
                lat_col = i
            if any(keyword in col_lower for keyword in ['lon', 'long', 'longitude']):
                lon_col = i
        
        # Usar fallback si no encuentra
        if lat_col is None:
            lat_col = 4
        if lon_col is None:
            lon_col = 5
            
        return lat_col, lon_col

    # ========== MÉTODOS REQUERIDOS POR BaseIndex ==========

    def insert_record(self, record):
        """Insertar registro con coordenadas"""
        try:
            if len(record) >= 6:
                lat = float(record[4])
                lon = float(record[5])
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    self.insert([lat, lon], record)
        except (ValueError, IndexError):
            pass

    def search(self, key):
        """Buscar por ID"""
        try:
            idx = int(key)
            obj = self.data_map.get(idx)
            return [obj] if obj is not None else []
        except (ValueError, TypeError):
            return []

    def remove(self, key):
        """Eliminar por ID con persistencia"""
        try:
            idx = int(key)
            if idx in self.data_map and idx in self.vector_map:
                vector = self.vector_map[idx]
                rect = tuple(vector + vector)
                
                # Eliminar del R-Tree
                self.idx.delete(idx, rect)
                
                # Eliminar de mapas
                del self.vector_map[idx]
                del self.data_map[idx]
                
                # Sincronizar cambios
                self._sync_to_disk()
                
                return [f"Removed item {idx}"]
        except Exception as e:
            print(f"Error removing from R-Tree: {e}")
        return []

    def __del__(self):
        """Asegurar persistencia al destruir el objeto"""
        try:
            self._sync_to_disk()
        except:
            pass