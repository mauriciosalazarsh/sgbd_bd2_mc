# multimedia/codebook/builder.py - Versión mínima
import numpy as np
import pickle
import os
from typing import List, Tuple, Optional

try:
    from sklearn.cluster import KMeans, MiniBatchKMeans
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    KMeans = MiniBatchKMeans = StandardScaler = None

class CodebookBuilder:
    def __init__(self, n_clusters=256, use_minibatch=True, random_state=42):
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn no está instalado")
        self.n_clusters = n_clusters
        self.use_minibatch = use_minibatch
        self.random_state = random_state
        self.kmeans = None
        self.scaler = StandardScaler() # type: ignore
        self.codebook = None
        self.is_fitted = False
    
    def build_codebook(self, features_data, normalize=True, save_path=None):
        print("Construyendo codebook...")
        # Implementación básica
        all_descriptors = []
        for file_path, features in features_data:
            if features.ndim == 1:
                all_descriptors.append(features)
            else:
                for descriptor in features:
                    all_descriptors.append(descriptor)
        
        descriptors = np.vstack(all_descriptors)
        
        if normalize:
            descriptors = self.scaler.fit_transform(descriptors)
        
        if self.use_minibatch:
            self.kmeans = MiniBatchKMeans(n_clusters=self.n_clusters, random_state=self.random_state) # type: ignore
        else:
            self.kmeans = KMeans(n_clusters=self.n_clusters, random_state=self.random_state) # type: ignore
        
        self.kmeans.fit(descriptors)
        self.codebook = self.kmeans.cluster_centers_
        self.is_fitted = True
        
        print(f"Codebook construido: {self.n_clusters} clusters")
        return self.codebook
    
    def create_bow_histogram(self, features, normalize=True):
        if not self.is_fitted:
            raise ValueError("Codebook no entrenado")
        
        if features.ndim == 1:
            features = features.reshape(1, -1)
        
        if normalize:
            features = self.scaler.transform(features)
        
        word_assignments = self.kmeans.predict(features) # type: ignore
        histogram = np.zeros(self.n_clusters, dtype=np.float32)
        
        for word_id in word_assignments:
            histogram[word_id] += 1
        
        if histogram.sum() > 0:
            histogram = histogram / histogram.sum()
        
        return histogram
    
    def create_histograms_batch(self, features_data, normalize=True):
        histograms = []
        for file_path, features in features_data:
            histogram = self.create_bow_histogram(features, normalize)
            histograms.append((file_path, histogram))
        return histograms
    
    def save_codebook(self, save_path):
        if not self.is_fitted:
            return
        data = {
            'kmeans': self.kmeans,
            'scaler': self.scaler,
            'codebook': self.codebook,
            'n_clusters': self.n_clusters,
            'is_fitted': self.is_fitted
        }
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as f:
            pickle.dump(data, f)
        print(f"Codebook guardado: {save_path}")
    
    def load_codebook(self, load_path):
        with open(load_path, 'rb') as f:
            data = pickle.load(f)
        self.kmeans = data['kmeans']
        self.scaler = data['scaler']
        self.codebook = data['codebook']
        self.n_clusters = data['n_clusters']
        self.is_fitted = data['is_fitted']
        print(f"Codebook cargado: {load_path}")
    
    def get_word_statistics(self, features_data):
        return {
            'words_used': self.n_clusters,
            'total_words': self.n_clusters,
            'coverage': 1.0
        }
