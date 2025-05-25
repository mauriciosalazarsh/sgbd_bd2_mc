from abc import ABC, abstractmethod
from typing import List

class BaseIndex(ABC):
    """
    Clase base abstracta para índices. Todos los métodos deben devolver listas.
    """
    # Sobrescrito en índices que usan una columna: siempre entero
    field_index: int

    @abstractmethod
    def insert(self, key, values) -> None:
        """Inserta un registro."""
        pass

    @abstractmethod
    def scan_all(self) -> List[str]:
        """Devuelve todos los registros como lista de strings."""
        pass

    def load_csv(self, path: str) -> None:
        """Carga un CSV; override en índices que lo soportan."""
        raise NotImplementedError("Este índice no soporta 'load_csv'.")

    def search(self, key: str) -> List[str]:
        """Búsqueda por clave exacta; override si se soporta."""
        raise NotImplementedError("Este índice no soporta 'search'.")

    def range_search(self, begin, end) -> List[str]:
        """Búsqueda por rango; override si se soporta."""
        raise NotImplementedError("Este índice no soporta 'range_search'.")

    def remove(self, key: str) -> List[str]:
        """Eliminación por clave; override si se soporta."""
        raise NotImplementedError("Este índice no soporta 'remove'.")