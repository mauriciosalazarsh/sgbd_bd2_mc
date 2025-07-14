# multimedia/feature_extractors/image_extractor.py - Versión corregida para Pylance
import numpy as np
import pickle
import os
from typing import List, Tuple, Optional, Any, Union

# Importaciones seguras para evitar errores
try:
    import cv2
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    cv2 = None

try:
    from keras.applications import ResNet50, InceptionV3
    from keras.applications.resnet50 import preprocess_input as resnet_preprocess
    from keras.applications.inception_v3 import preprocess_input as inception_preprocess
    from keras.preprocessing import image
    TENSORFLOW_AVAILABLE = True
except ImportError:
    try:
        # Fallback para TensorFlow 2.x
        from keras.applications import ResNet50, InceptionV3
        from keras.applications.resnet50 import preprocess_input as resnet_preprocess
        from keras.applications.inception_v3 import preprocess_input as inception_preprocess
        from keras.preprocessing import image
        TENSORFLOW_AVAILABLE = True
    except ImportError:
        TENSORFLOW_AVAILABLE = False
        ResNet50 = InceptionV3 = None
        resnet_preprocess = inception_preprocess = None
        image = None

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None

class ImageFeatureExtractor:
    def __init__(self, method='sift'):
        """
        Extractor de características para imágenes
        
        Args:
            method: 'sift', 'resnet50', 'inception_v3'
        """
        self.method = method.lower()
        self.model: Optional[Any] = None
        self.sift: Optional[Any] = None
        
        # Validar dependencias según el método
        if self.method == 'sift':
            if not CV2_AVAILABLE or cv2 is None:
                raise ImportError("OpenCV no está instalado. Ejecuta: pip install opencv-python")
            
            # Intentar crear SIFT con diferentes métodos según la versión de OpenCV
            self.sift = self._create_sift_detector()
            if self.sift is None:
                raise ImportError("SIFT no está disponible en esta versión de OpenCV")
                    
        elif self.method in ['resnet50', 'inception_v3']:
            if not TENSORFLOW_AVAILABLE:
                raise ImportError(f"TensorFlow no está instalado. Ejecuta: pip install tensorflow")
            
            if self.method == 'resnet50' and ResNet50 is not None:
                self.model = ResNet50(weights='imagenet', include_top=False, pooling='avg')
            elif self.method == 'inception_v3' and InceptionV3 is not None:
                self.model = InceptionV3(weights='imagenet', include_top=False, pooling='avg')
            else:
                raise ImportError(f"No se pudo cargar el modelo {self.method}")
        else:
            raise ValueError(f"Método '{method}' no soportado. Use: 'sift', 'resnet50', 'inception_v3'")
    
    def _create_sift_detector(self) -> Optional[Any]:
        """Crea detector SIFT manejando diferentes versiones de OpenCV"""
        if not CV2_AVAILABLE or cv2 is None:
            return None
            
        # Método 1: OpenCV 4.x estándar
        try:
            if hasattr(cv2, 'SIFT_create'):
                return cv2.SIFT_create()  # type: ignore
        except AttributeError:
            pass
        
        # Método 2: OpenCV con xfeatures2d
        try:
            if hasattr(cv2, 'xfeatures2d'):
                xfeatures2d = getattr(cv2, 'xfeatures2d')
                if hasattr(xfeatures2d, 'SIFT_create'):
                    return xfeatures2d.SIFT_create()  # type: ignore
        except (AttributeError, Exception):
            pass
        
        # Método 3: Versiones muy antiguas
        try:
            if hasattr(cv2, 'SIFT'):
                return cv2.SIFT()  # type: ignore
        except (AttributeError, Exception):
            pass
        
        # Método 4: Intentar crear dinámicamente
        try:
            # Usar getattr para evitar errores de type checking
            sift_create = getattr(cv2, 'SIFT_create', None)
            if sift_create and callable(sift_create):
                return sift_create()
        except Exception:
            pass
        
        return None
    
    def extract_sift_features(self, image_path: str) -> Optional[np.ndarray]:
        """Extrae características SIFT de una imagen"""
        if not CV2_AVAILABLE or cv2 is None or self.sift is None:
            raise RuntimeError("SIFT no está disponible")
            
        try:
            img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
            if img is None:
                print(f"No se pudo cargar la imagen: {image_path}")
                return None
                
            keypoints, descriptors = self.sift.detectAndCompute(img, None)
            
            if descriptors is None:
                print(f"No se encontraron características SIFT en: {image_path}")
                # Crear descriptor dummy para evitar errores
                return np.random.rand(1, 128).astype(np.float32)
                
            return descriptors
            
        except Exception as e:
            print(f"Error extrayendo SIFT de {image_path}: {e}")
            return None
    
    def extract_cnn_features(self, image_path: str) -> Optional[np.ndarray]:
        """Extrae características CNN (ResNet50 o InceptionV3)"""
        if not TENSORFLOW_AVAILABLE or self.model is None or image is None:
            raise RuntimeError("TensorFlow no está disponible")
            
        try:
            if self.method == 'resnet50':
                target_size = (224, 224)
                preprocess_func = resnet_preprocess
            else:  # inception_v3
                target_size = (299, 299)
                preprocess_func = inception_preprocess
            
            # Cargar y procesar imagen
            img = image.load_img(image_path, target_size=target_size)
            img_array = image.img_to_array(img)
            img_array = np.expand_dims(img_array, axis=0)
            if preprocess_func is None:
                raise RuntimeError(f"La función de preprocesamiento para {self.method} no está disponible.")
            img_array = preprocess_func(img_array)
            
            # Extraer características - manejo de diferentes versiones de Keras
            try:
                # TensorFlow 2.x
                features = self.model.predict(img_array, verbose=0)
            except TypeError:
                # Versiones más antiguas que no soportan verbose=0
                features = self.model.predict(img_array)
            
            return features.flatten()
            
        except Exception as e:
            print(f"Error extrayendo CNN de {image_path}: {e}")
            return None
    
    def extract_features(self, image_path: str) -> Optional[np.ndarray]:
        """Extrae características según el método configurado"""
        if not os.path.exists(image_path):
            print(f"Archivo no encontrado: {image_path}")
            return None
            
        if self.method == 'sift':
            return self.extract_sift_features(image_path)
        else:
            return self.extract_cnn_features(image_path)
    
    def extract_features_batch(self, image_paths: List[str]) -> List[Tuple[str, np.ndarray]]:
        """Extrae características de múltiples imágenes"""
        results = []
        total = len(image_paths)
        
        print(f"Extrayendo características de {total} imágenes usando {self.method.upper()}...")
        
        for i, path in enumerate(image_paths, 1):
            if i % 10 == 0 or i == total:
                print(f"Progreso: {i}/{total} ({i/total*100:.1f}%)")
                
            features = self.extract_features(path)
            if features is not None:
                results.append((path, features))
            else:
                print(f" No se pudieron extraer características de: {path}")
        
        print(f" Características extraídas exitosamente: {len(results)}/{total}")
        return results
    
    def save_features(self, features_data: List[Tuple[str, np.ndarray]], output_path: str):
        """Guarda las características extraídas en un archivo"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'wb') as f:
                pickle.dump(features_data, f)
            print(f" Características guardadas en: {output_path}")
        except Exception as e:
            print(f" Error guardando características: {e}")
    
    def load_features(self, input_path: str) -> List[Tuple[str, np.ndarray]]:
        """Carga características desde un archivo"""
        try:
            with open(input_path, 'rb') as f:
                data = pickle.load(f)
            print(f" Características cargadas desde: {input_path}")
            return data
        except Exception as e:
            print(f" Error cargando características: {e}")
            return []
    
    @staticmethod
    def get_available_methods() -> List[str]:
        """Retorna los métodos disponibles según las dependencias instaladas"""
        methods = []
        
        if CV2_AVAILABLE and cv2 is not None:
            # Verificar si SIFT está disponible
            temp_extractor = ImageFeatureExtractor.__new__(ImageFeatureExtractor)
            temp_extractor.method = 'sift'
            if temp_extractor._create_sift_detector() is not None:
                methods.append('sift')
        
        if TENSORFLOW_AVAILABLE:
            methods.extend(['resnet50', 'inception_v3'])
        
        return methods
    
    def get_feature_info(self) -> dict:
        """Retorna información sobre el extractor configurado"""
        return {
            'method': self.method,
            'cv2_available': CV2_AVAILABLE,
            'tensorflow_available': TENSORFLOW_AVAILABLE,
            'pil_available': PIL_AVAILABLE,
            'model_loaded': self.model is not None,
            'sift_available': self.sift is not None
        }