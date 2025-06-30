import re
import string
from typing import List, Set
import nltk
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from nltk.tokenize import word_tokenize
import unicodedata

class TextPreprocessor:
    """
    Clase para preprocesamiento de texto según los requerimientos del proyecto:
    - Tokenización
    - Filtrar stopwords
    - Eliminar signos innecesarios
    - Stemming
    """
    
    def __init__(self, language='spanish'):
        """
        Inicializa el preprocesador
        
        Args:
            language: Idioma para stopwords y stemming ('spanish' o 'english')
        """
        self.language = language
        self._download_nltk_data()
        self._setup_components()
    
    def _download_nltk_data(self):
        """Descarga los recursos necesarios de NLTK"""
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt')
        
        try:
            nltk.data.find('corpora/stopwords')
        except LookupError:
            nltk.download('stopwords')
    
    def _setup_components(self):
        """Configura los componentes de procesamiento"""
        # Stemmer
        if self.language == 'spanish':
            self.stemmer = SnowballStemmer('spanish')
        else:
            self.stemmer = SnowballStemmer('english')
        
        # Stopwords
        try:
            self.stop_words = set(stopwords.words(self.language))
        except:
            # Fallback a inglés si no está disponible el idioma
            self.stop_words = set(stopwords.words('english'))
        
        # Agregar stopwords personalizadas comunes
        custom_stops = {'el', 'la', 'de', 'que', 'y', 'a', 'en', 'un', 'es', 'se', 
                       'no', 'te', 'lo', 'le', 'da', 'su', 'por', 'son', 'con', 
                       'para', 'al', 'del', 'los', 'las', 'una', 'como', 'todo'}
        self.stop_words.update(custom_stops)
    
    def normalize_text(self, text: str) -> str:
        """
        Normaliza el texto básico
        
        Args:
            text: Texto a normalizar
            
        Returns:
            Texto normalizado
        """
        if not text:
            return ""
        
        # Convertir a lowercase
        text = text.lower()
        
        # Normalizar caracteres unicode (quitar acentos)
        text = unicodedata.normalize('NFD', text)
        text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
        
        return text
    
    def remove_unwanted_chars(self, text: str) -> str:
        """
        Elimina signos de puntuación y caracteres no deseados
        
        Args:
            text: Texto a limpiar
            
        Returns:
            Texto limpio
        """
        # Eliminar URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Eliminar emails
        text = re.sub(r'\S+@\S+', '', text)
        
        # Eliminar números solos (pero mantener palabras con números)
        text = re.sub(r'\b\d+\b', '', text)
        
        # Eliminar puntuación pero mantener espacios
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Eliminar espacios múltiples
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokeniza el texto en palabras
        
        Args:
            text: Texto a tokenizar
            
        Returns:
            Lista de tokens
        """
        try:
            tokens = word_tokenize(text, language=self.language)
        except:
            # Fallback manual si falla NLTK
            tokens = text.split()
        
        # Filtrar tokens muy cortos
        tokens = [token for token in tokens if len(token) >= 2]
        
        return tokens
    
    def remove_stopwords(self, tokens: List[str]) -> List[str]:
        """
        Elimina stopwords de la lista de tokens
        
        Args:
            tokens: Lista de tokens
            
        Returns:
            Lista de tokens sin stopwords
        """
        return [token for token in tokens if token not in self.stop_words]
    
    def stem_tokens(self, tokens: List[str]) -> List[str]:
        """
        Aplica stemming a los tokens
        
        Args:
            tokens: Lista de tokens
            
        Returns:
            Lista de tokens con stemming aplicado
        """
        return [self.stemmer.stem(token) for token in tokens]
    
    def preprocess(self, text: str) -> List[str]:
        """
        Proceso completo de preprocesamiento
        
        Args:
            text: Texto a procesar
            
        Returns:
            Lista de tokens procesados
        """
        if not text or not isinstance(text, str):
            return []
        
        # 1. Normalizar texto
        text = self.normalize_text(text)
        
        # 2. Eliminar caracteres no deseados
        text = self.remove_unwanted_chars(text)
        
        # 3. Tokenizar
        tokens = self.tokenize(text)
        
        # 4. Eliminar stopwords
        tokens = self.remove_stopwords(tokens)
        
        # 5. Aplicar stemming
        tokens = self.stem_tokens(tokens)
        
        # 6. Filtrar tokens vacíos
        tokens = [token for token in tokens if token.strip()]
        
        return tokens
    
    def preprocess_documents(self, documents: List[str]) -> List[List[str]]:
        """
        Procesa múltiples documentos
        
        Args:
            documents: Lista de textos/documentos
            
        Returns:
            Lista de listas de tokens procesados
        """
        return [self.preprocess(doc) for doc in documents]
    
    def concatenate_fields(self, record: dict, text_fields: List[str]) -> str:
        """
        Concatena campos textuales de un registro
        
        Args:
            record: Diccionario con los datos del registro
            text_fields: Lista de nombres de campos a concatenar
            
        Returns:
            Texto concatenado
        """
        texts = []
        for field in text_fields:
            if field in record and record[field]:
                texts.append(str(record[field]))
        
        return ' '.join(texts)

# Función helper para uso directo
def quick_preprocess(text: str, language: str = 'spanish') -> List[str]:
    """
    Función helper para preprocesamiento rápido
    
    Args:
        text: Texto a procesar
        language: Idioma
        
    Returns:
        Lista de tokens procesados
    """
    processor = TextPreprocessor(language)
    return processor.preprocess(text)