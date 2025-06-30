"""
Índices del proyecto de base de datos multimedia
"""

# Imports de índices base (siempre disponibles)
try:
    from .base_index import BaseIndex
    BASE_INDEX_AVAILABLE = True
except ImportError:
    BaseIndex = None
    BASE_INDEX_AVAILABLE = False

# Imports de índices principales (usando nombres reales de tus clases)
try:
    from .sequential import SequentialFile  # Es SequentialFile, no SequentialIndex
    SEQUENTIAL_AVAILABLE = True
except ImportError:
    SequentialFile = None
    SEQUENTIAL_AVAILABLE = False

try:
    from .btree import BPlusTree  # Es BPlusTree, no BTreeIndex
    BTREE_AVAILABLE = True
except ImportError:
    BPlusTree = None
    BTREE_AVAILABLE = False

try:
    from .hash_extensible import ExtendibleHash  # Es ExtendibleHash, no ExtensibleHashIndex
    HASH_AVAILABLE = True
except ImportError:
    ExtendibleHash = None
    HASH_AVAILABLE = False

try:
    from .isam import ISAM  # Es ISAM, no ISAMIndex
    ISAM_AVAILABLE = True
except ImportError:
    ISAM = None
    ISAM_AVAILABLE = False

try:
    from .rtree import MultidimensionalRTree  # Es MultidimensionalRTree, no RTreeIndex
    RTREE_AVAILABLE = True
except ImportError:
    MultidimensionalRTree = None
    RTREE_AVAILABLE = False

# Imports de nuevos índices (con manejo de errores)
try:
    from .inverted_index import InvertedIndex
    INVERTED_INDEX_AVAILABLE = True
except ImportError as e:
    print(f"Info: InvertedIndex no disponible: {e}")
    InvertedIndex = None
    INVERTED_INDEX_AVAILABLE = False

try:
    from .spimi import SPIMIIndexBuilder
    SPIMI_AVAILABLE = True
except ImportError as e:
    print(f"Info: SPIMI no disponible: {e}")
    SPIMIIndexBuilder = None
    SPIMI_AVAILABLE = False

# Lista dinámica de exports (usando nombres reales)
__all__ = []

if BASE_INDEX_AVAILABLE:
    __all__.append('BaseIndex')
if SEQUENTIAL_AVAILABLE:
    __all__.append('SequentialFile')  # Nombre real
if BTREE_AVAILABLE:
    __all__.append('BPlusTree')  # Nombre real
if HASH_AVAILABLE:
    __all__.append('ExtendibleHash')  # Nombre real
if ISAM_AVAILABLE:
    __all__.append('ISAM')  # Nombre real
if RTREE_AVAILABLE:
    __all__.append('MultidimensionalRTree')  # Nombre real
if INVERTED_INDEX_AVAILABLE:
    __all__.append('InvertedIndex')
if SPIMI_AVAILABLE:
    __all__.append('SPIMIIndexBuilder')