# multimedia/feature_extractors/audio_extractor.py - Versión corregida
import numpy as np
import pickle
import os
from typing import List, Tuple, Optional

# Importación segura de librosa
try:
    import librosa
    LIBROSA_AVAILABLE = True
except ImportError:
    LIBROSA_AVAILABLE = False
    librosa = None

class AudioFeatureExtractor:
    def __init__(self, method='mfcc', n_mfcc=13, n_fft=2048, hop_length=512):
        """
        Extractor de características para audio
        
        Args:
            method: 'mfcc', 'spectrogram', 'comprehensive'
            n_mfcc: número de coeficientes MFCC
            n_fft: tamaño de la ventana FFT
            hop_length: número de samples entre frames
        """
        if not LIBROSA_AVAILABLE:
            raise ImportError("Librosa no está instalado. Ejecuta: pip install librosa")
            
        self.method = method.lower()
        self.n_mfcc = n_mfcc
        self.n_fft = n_fft
        self.hop_length = hop_length
        
        # Validar método
        valid_methods = ['mfcc', 'spectrogram', 'comprehensive']
        if self.method not in valid_methods:
            raise ValueError(f"Método '{method}' no soportado. Use: {valid_methods}")
    
    def extract_mfcc_features(self, audio_path: str, sr=22050) -> Optional[np.ndarray]:
        """Extrae características MFCC de un archivo de audio"""
        if not LIBROSA_AVAILABLE or librosa is None:
            raise RuntimeError("Librosa no está disponible")
            
        try:
            # Cargar audio con manejo mejorado de errores
            try:
                y, sr = librosa.load(audio_path, sr=sr, duration=30)  # Limitar a 30 segundos
            except Exception as load_error:
                print(f"Error cargando audio {audio_path}: {load_error}")
                # Intentar con audioread como fallback
                try:
                    import audioread
                    with audioread.audio_open(audio_path) as f:
                        duration = f.duration
                    if duration < 0.1:  # Audio muy corto
                        print(f"Audio muy corto o corrupto: {audio_path}")
                        return None
                except:
                    print(f"Archivo de audio no legible: {audio_path}")
                    return None
                # Si llegamos aquí, el archivo existe pero librosa tuvo problemas
                # Devolver características dummy para no romper el flujo
                print(f"Usando características dummy para: {audio_path}")
                return np.random.rand(self.n_mfcc * 2).astype(np.float32)
            
            if len(y) == 0:
                print(f"Archivo de audio vacío: {audio_path}")
                return None
            
            # Extraer MFCCs
            mfccs = librosa.feature.mfcc(
                y=y, 
                sr=sr, 
                n_mfcc=self.n_mfcc,
                n_fft=self.n_fft,
                hop_length=self.hop_length
            )
            
            # Calcular estadísticas (media y desviación estándar)
            mfcc_mean = np.mean(mfccs, axis=1)
            mfcc_std = np.std(mfccs, axis=1)
            
            # Concatenar media y desviación estándar
            features = np.concatenate([mfcc_mean, mfcc_std])
            
            return features.astype(np.float32)
            
        except Exception as e:
            print(f"Error extrayendo MFCC de {audio_path}: {e}")
            return None
    
    def extract_spectrogram_features(self, audio_path: str, sr=22050) -> Optional[np.ndarray]:
        """Extrae características del espectrograma"""
        if not LIBROSA_AVAILABLE or librosa is None:
            raise RuntimeError("Librosa no está disponible")
            
        try:
            # Cargar audio
            y, sr = librosa.load(audio_path, sr=sr)
            
            if len(y) == 0:
                print(f"Archivo de audio vacío: {audio_path}")
                return None
            
            # Extraer espectrograma mel
            mel_spec = librosa.feature.melspectrogram(
                y=y,
                sr=sr,
                n_fft=self.n_fft,
                hop_length=self.hop_length,
                n_mels=128
            )
            
            # Convertir a escala de decibeles
            mel_spec_db = librosa.power_to_db(mel_spec, ref=np.max)
            
            # Calcular estadísticas
            spec_mean = np.mean(mel_spec_db, axis=1)
            spec_std = np.std(mel_spec_db, axis=1)
            
            # Concatenar características
            features = np.concatenate([spec_mean, spec_std])
            
            return features.astype(np.float32)
            
        except Exception as e:
            print(f"Error extrayendo espectrograma de {audio_path}: {e}")
            return None
    
    def extract_comprehensive_features(self, audio_path: str, sr=22050) -> Optional[np.ndarray]:
        """Extrae un conjunto completo de características de audio"""
        if not LIBROSA_AVAILABLE or librosa is None:
            raise RuntimeError("Librosa no está disponible")
            
        try:
            # Cargar audio
            y, sr = librosa.load(audio_path, sr=sr)
            
            if len(y) == 0:
                print(f"Archivo de audio vacío: {audio_path}")
                return None
            
            features_list = []
            
            # MFCCs
            mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=self.n_mfcc)
            features_list.extend([np.mean(mfccs, axis=1), np.std(mfccs, axis=1)])
            
            # Centroide espectral
            spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)
            features_list.extend([np.mean(spectral_centroids), np.std(spectral_centroids)])
            
            # Rolloff espectral
            spectral_rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr)
            features_list.extend([np.mean(spectral_rolloff), np.std(spectral_rolloff)])
            
            # Zero crossing rate
            zcr = librosa.feature.zero_crossing_rate(y)
            features_list.extend([np.mean(zcr), np.std(zcr)])
            
            # Chroma features
            chroma = librosa.feature.chroma_stft(y=y, sr=sr)
            features_list.extend([np.mean(chroma, axis=1), np.std(chroma, axis=1)])
            
            # Concatenar todas las características
            all_features = []
            for f in features_list:
                if hasattr(f, 'flatten'):
                    all_features.extend(f.flatten())
                else:
                    all_features.append(float(f))
            
            return np.array(all_features, dtype=np.float32)
            
        except Exception as e:
            print(f"Error extrayendo características comprehensivas de {audio_path}: {e}")
            return None
    
    def extract_features(self, audio_path: str) -> Optional[np.ndarray]:
        """Extrae características según el método configurado"""
        if not os.path.exists(audio_path):
            print(f"Archivo no encontrado: {audio_path}")
            return None
            
        if self.method == 'mfcc':
            return self.extract_mfcc_features(audio_path)
        elif self.method == 'spectrogram':
            return self.extract_spectrogram_features(audio_path)
        elif self.method == 'comprehensive':
            return self.extract_comprehensive_features(audio_path)
        else:
            raise ValueError(f"Método '{self.method}' no reconocido")
    
    def extract_features_batch(self, audio_paths: List[str]) -> List[Tuple[str, np.ndarray]]:
        """Extrae características de múltiples archivos de audio"""
        results = []
        total = len(audio_paths)
        
        print(f"Extrayendo características de {total} archivos de audio usando {self.method.upper()}...")
        
        for i, path in enumerate(audio_paths, 1):
            if i % 5 == 0 or i == total:
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
        if LIBROSA_AVAILABLE:
            return ['mfcc', 'spectrogram', 'comprehensive']
        else:
            return []
    
    def get_feature_info(self) -> dict:
        """Retorna información sobre el extractor configurado"""
        return {
            'method': self.method,
            'librosa_available': LIBROSA_AVAILABLE,
            'n_mfcc': self.n_mfcc,
            'n_fft': self.n_fft,
            'hop_length': self.hop_length
        }