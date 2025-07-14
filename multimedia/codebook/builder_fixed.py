# multimedia/codebook/builder_fixed.py - Fixed version for audio features
import numpy as np
import pickle
import os
from typing import List, Tuple, Optional, Union

try:
    from sklearn.cluster import KMeans, MiniBatchKMeans
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    KMeans = MiniBatchKMeans = StandardScaler = None

class CodebookBuilder:
    def __init__(self, n_clusters=256, use_minibatch=True, random_state=42, feature_type='image'):
        """
        Initialize codebook builder
        
        Args:
            n_clusters: Number of clusters for codebook
            use_minibatch: Use MiniBatchKMeans for efficiency
            random_state: Random seed
            feature_type: 'image' or 'audio' - determines how features are processed
        """
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn no está instalado")
        self.n_clusters = n_clusters
        self.use_minibatch = use_minibatch
        self.random_state = random_state
        self.feature_type = feature_type
        self.kmeans = None
        self.scaler = StandardScaler() # type: ignore
        self.codebook = None
        self.is_fitted = False
        self.audio_feature_dim = None  # For audio direct features
    
    def build_codebook(self, features_data, normalize=True, save_path=None):
        """Build codebook from features"""
        print(f"Construyendo codebook para {self.feature_type}...")
        
        if self.feature_type == 'audio':
            # For audio: use features directly, no clustering needed
            # Just fit the scaler for normalization
            print("Audio detectado: usando características directas sin clustering")
            audio_features = []
            for file_path, features in features_data:
                if features.ndim == 1:
                    audio_features.append(features)
            
            if audio_features:
                audio_features_matrix = np.vstack(audio_features)
                self.audio_feature_dim = audio_features_matrix.shape[1]
                
                if normalize:
                    self.scaler.fit(audio_features_matrix)
                
                self.is_fitted = True
                self.codebook = None  # No codebook for audio
                print(f"Audio: dimensión de características = {self.audio_feature_dim}")
            
        else:
            # For images: standard bag-of-words clustering
            all_descriptors = []
            for file_path, features in features_data:
                if features.ndim == 1:
                    # If 1D, it might be a summary feature - skip or handle differently
                    print(f" Advertencia: características 1D encontradas para {file_path}")
                    continue
                else:
                    # 2D array: multiple descriptors (e.g., SIFT keypoints)
                    for descriptor in features:
                        all_descriptors.append(descriptor)
            
            if not all_descriptors:
                raise ValueError("No se encontraron descriptores válidos para clustering")
            
            descriptors = np.vstack(all_descriptors)
            print(f"Total descriptores para clustering: {len(descriptors)}")
            
            if normalize:
                descriptors = self.scaler.fit_transform(descriptors)
            
            # Perform clustering
            if self.use_minibatch:
                self.kmeans = MiniBatchKMeans(n_clusters=self.n_clusters, random_state=self.random_state) # type: ignore
            else:
                self.kmeans = KMeans(n_clusters=self.n_clusters, random_state=self.random_state) # type: ignore
            
            self.kmeans.fit(descriptors)
            self.codebook = self.kmeans.cluster_centers_
            self.is_fitted = True
            
            print(f"Codebook construido: {self.n_clusters} clusters")
        
        if save_path:
            self.save_codebook(save_path)
        
        return self.codebook
    
    def create_bow_histogram(self, features, normalize=True):
        """Create bag-of-words histogram from features"""
        if not self.is_fitted:
            raise ValueError("Codebook no entrenado")
        
        if self.feature_type == 'audio':
            # For audio: return normalized features directly as "histogram"
            if features.ndim == 1:
                histogram = features.copy()
                if normalize and self.scaler:
                    histogram = self.scaler.transform(histogram.reshape(1, -1))[0]
                
                # Ensure non-negative values for histogram
                histogram = histogram - histogram.min()
                if histogram.sum() > 0:
                    histogram = histogram / histogram.sum()
                
                return histogram.astype(np.float32)
            else:
                raise ValueError("Audio features should be 1D array")
        
        else:
            # For images: standard bag-of-words
            if features.ndim == 1:
                # This shouldn't happen for image features
                print(" Warning: 1D features for image - treating as single descriptor")
                features = features.reshape(1, -1)
            
            if normalize:
                features = self.scaler.transform(features)
            
            # Assign each descriptor to nearest cluster
            word_assignments = self.kmeans.predict(features) # type: ignore
            histogram = np.zeros(self.n_clusters, dtype=np.float32)
            
            # Count occurrences
            for word_id in word_assignments:
                histogram[word_id] += 1
            
            # Normalize histogram
            if histogram.sum() > 0:
                histogram = histogram / histogram.sum()
            
            return histogram
    
    def create_histograms_batch(self, features_data, normalize=True):
        """Create histograms for multiple files"""
        histograms = []
        for file_path, features in features_data:
            try:
                histogram = self.create_bow_histogram(features, normalize)
                histograms.append((file_path, histogram))
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
        return histograms
    
    def save_codebook(self, save_path):
        """Save codebook to disk"""
        if not self.is_fitted:
            return
        data = {
            'kmeans': self.kmeans,
            'scaler': self.scaler,
            'codebook': self.codebook,
            'n_clusters': self.n_clusters,
            'is_fitted': self.is_fitted,
            'feature_type': self.feature_type,
            'audio_feature_dim': self.audio_feature_dim
        }
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as f:
            pickle.dump(data, f)
        print(f"Codebook guardado: {save_path}")
    
    def load_codebook(self, load_path):
        """Load codebook from disk"""
        with open(load_path, 'rb') as f:
            data = pickle.load(f)
        self.kmeans = data.get('kmeans')
        self.scaler = data.get('scaler')
        self.codebook = data.get('codebook')
        self.n_clusters = data.get('n_clusters', 256)
        self.is_fitted = data.get('is_fitted', False)
        self.feature_type = data.get('feature_type', 'image')
        self.audio_feature_dim = data.get('audio_feature_dim')
        print(f"Codebook cargado: {load_path} (tipo: {self.feature_type})")
    
    def get_word_statistics(self, features_data):
        """Get statistics about codebook usage"""
        if self.feature_type == 'audio':
            return {
                'feature_type': 'audio',
                'feature_dimension': self.audio_feature_dim,
                'total_files': len(features_data)
            }
        else:
            return {
                'feature_type': 'image',
                'words_used': self.n_clusters,
                'total_words': self.n_clusters,
                'coverage': 1.0
            }