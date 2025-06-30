"""
Módulo de procesamiento de texto para el proyecto de base de datos multimedia.

Incluye:
- Preprocesamiento de texto (tokenización, stemming, stopwords)
- Cálculo de TF-IDF
- Funciones de similitud de coseno
"""

# Imports con manejo de errores para evitar problemas de dependencias
try:
    from .preprocessor import TextPreprocessor, quick_preprocess
    PREPROCESSOR_AVAILABLE = True
except ImportError as e:
    print(f"Warning: preprocessor no disponible: {e}")
    TextPreprocessor = None
    quick_preprocess = None
    PREPROCESSOR_AVAILABLE = False

try:
    from .tfidf import TFIDFCalculator, BatchTFIDFProcessor
    TFIDF_AVAILABLE = True
except ImportError as e:
    print(f"Warning: tfidf no disponible: {e}")
    TFIDFCalculator = None
    BatchTFIDFProcessor = None
    TFIDF_AVAILABLE = False

# Lista de exports con verificación
__all__ = []

if PREPROCESSOR_AVAILABLE:
    __all__.extend(['TextPreprocessor', 'quick_preprocess'])

if TFIDF_AVAILABLE:
    __all__.extend(['TFIDFCalculator', 'BatchTFIDFProcessor'])

# Información del módulo
__version__ = "1.0.0"
__author__ = "Proyecto Base de Datos Multimedia"