#!/usr/bin/env python3
"""
Script de configuración para el Sistema de Base de Datos Multimodal
Proyecto 2 - BDII
"""

import os
import sys
import subprocess
from pathlib import Path

def print_header(title):
    """Imprime un header con formato"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def check_python_version():
    """Verifica la versión de Python"""
    print_header("VERIFICANDO VERSIÓN DE PYTHON")
    
    version = sys.version_info
    print(f"Versión de Python: {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(" ERROR: Se requiere Python 3.8 o superior")
        return False
    
    print(" Versión de Python compatible")
    return True

def create_directories():
    """Crea los directorios necesarios"""
    print_header("CREANDO DIRECTORIOS")
    
    directories = [
        'datos',
        'multimedia_data',
        'indices',
        'logs'
    ]
    
    for directory in directories:
        path = Path(directory)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            print(f" Creado: {directory}")
        else:
            print(f" Ya existe: {directory}")

def install_core_dependencies():
    """Instala las dependencias básicas"""
    print_header("INSTALANDO DEPENDENCIAS BÁSICAS")
    
    core_deps = [
        'numpy>=1.21.0',
        'pandas>=1.3.0',
        'scikit-learn>=1.0.0',
        'nltk>=3.7',
        'tqdm>=4.64.0',
        'click>=8.0.0'
    ]
    
    for dep in core_deps:
        try:
            print(f"📦 Instalando {dep}...")
            subprocess.run([sys.executable, '-m', 'pip', 'install', dep], 
                         check=True, capture_output=True)
            print(f" {dep} instalado")
        except subprocess.CalledProcessError as e:
            print(f" Error instalando {dep}: {e}")
            return False
    
    return True

def install_optional_dependencies():
    """Instala dependencias opcionales con manejo de errores"""
    print_header("INSTALANDO DEPENDENCIAS OPCIONALES")
    
    optional_deps = {
        'opencv-python>=4.5.0': 'Procesamiento de imágenes (SIFT)',
        'librosa>=0.9.0': 'Procesamiento de audio',
        'tensorflow>=2.8.0': 'Deep Learning (ResNet50, InceptionV3)',
        'fastapi>=0.75.0': 'API web',
        'uvicorn>=0.17.0': 'Servidor web'
    }
    
    installed = []
    failed = []
    
    for dep, description in optional_deps.items():
        try:
            print(f"📦 Instalando {dep} ({description})...")
            subprocess.run([sys.executable, '-m', 'pip', 'install', dep], 
                         check=True, capture_output=True, timeout=300)
            print(f" {dep} instalado")
            installed.append(dep)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f" No se pudo instalar {dep}: {e}")
            failed.append(dep)
    
    print(f"\n Resumen:")
    print(f"   Instaladas: {len(installed)}")
    print(f"   Fallidas: {len(failed)}")
    
    if failed:
        print(f"\n Para instalar manualmente las que fallaron:")
        for dep in failed:
            print(f"  pip install {dep}")

def download_nltk_data():
    """Descarga datos de NLTK"""
    print_header("CONFIGURANDO NLTK")
    
    try:
        import nltk
        
        # Datos básicos de NLTK
        nltk_data = ['punkt', 'stopwords', 'wordnet', 'omw-1.4']
        
        for data in nltk_data:
            try:
                print(f"📥 Descargando {data}...")
                nltk.download(data, quiet=True)
                print(f" {data} descargado")
            except Exception as e:
                print(f" Error descargando {data}: {e}")
                
    except ImportError:
        print(" NLTK no está instalado, saltando configuración")

def verify_installation():
    """Verifica que las dependencias estén correctamente instaladas"""
    print_header("VERIFICANDO INSTALACIÓN")
    
    # Verificaciones básicas
    checks = {
        'numpy': 'import numpy; print(f"NumPy {numpy.__version__}")',
        'pandas': 'import pandas; print(f"Pandas {pandas.__version__}")',
        'sklearn': 'import sklearn; print(f"Scikit-learn {sklearn.__version__}")',
        'nltk': 'import nltk; print(f"NLTK {nltk.__version__}")'
    }
    
    # Verificaciones opcionales - simplificadas para evitar problemas de versión
    optional_checks = {
        'cv2': 'import cv2; print(f"OpenCV {cv2.__version__}")',
        'librosa': 'import librosa; print(f"Librosa {librosa.__version__}")',
        'tensorflow': 'import tensorflow as tf',  # Simplificado
        'fastapi': 'import fastapi; print(f"FastAPI {fastapi.__version__}")'
    }
    
    print(" Dependencias básicas:")
    for name, code in checks.items():
        try:
            exec(code)
            print(f"   {name}")
        except ImportError:
            print(f"   {name} NO DISPONIBLE")
    
    print("\n Dependencias opcionales:")
    for name, code in optional_checks.items():
        try:
            exec(code)
            print(f"   {name}")
        except ImportError:
            print(f"   {name} no disponible")
        except AttributeError as e:
            # Manejo especial para TensorFlow y otras librerías con versiones complejas
            if name == 'tensorflow':
                try:
                    import tensorflow as tf
                    # Intentar diferentes formas de obtener la versión
                    version = getattr(tf, '__version__', 'unknown')
                    print(f"TensorFlow {version}")
                    print(f"   {name}")
                except:
                    print(f"   {name} (versión no detectada)")
            else:
                print(f"   {name} problema de versión: {e}")

def test_system():
    """Prueba básica del sistema"""
    print_header("PRUEBA DEL SISTEMA")
    
    try:
        # Importar el motor principal
        from engine import Engine
        print(" Motor tradicional importado correctamente")
        
        # Crear instancia
        engine = Engine()
        print(" Motor tradicional inicializado")
        
        # Probar motor multimedia si está disponible
        try:
            from multimedia.multimedia_engine import MultimediaEngine
            print(" Motor multimedia importado correctamente")
            
            # Verificar métodos disponibles
            from multimedia.feature_extractors.image_extractor import ImageFeatureExtractor
            available_methods = ImageFeatureExtractor.get_available_methods()
            print(f" Métodos de imagen disponibles: {available_methods}")
            
            from multimedia.feature_extractors.audio_extractor import AudioFeatureExtractor
            available_methods = AudioFeatureExtractor.get_available_methods()
            print(f" Métodos de audio disponibles: {available_methods}")
            
        except ImportError as e:
            print(f" Error importando motor multimedia: {e}")
        
    except ImportError as e:
        print(f" Error importando sistema: {e}")
        return False
    
    print("\n🎉 Sistema listo para usar!")
    return True

def create_example_data():
    """Crea datos de ejemplo para probar el sistema"""
    print_header("CREANDO DATOS DE EJEMPLO")
    
    try:
        import pandas as pd
        import numpy as np
        
        # Crear dataset de ejemplo
        np.random.seed(42)
        n_records = 100
        
        data = {
            'id': range(1, n_records + 1),
            'name': [f'Product_{i}' for i in range(1, n_records + 1)],
            'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Sports'], n_records),
            'price': np.random.uniform(10, 1000, n_records).round(2),
            'rating': np.random.uniform(1, 5, n_records).round(1),
            'description': [f'Description for product {i}' for i in range(1, n_records + 1)]
        }
        
        df = pd.DataFrame(data)
        
        # Guardar en CSV
        example_path = Path('datos/example_products.csv')
        df.to_csv(example_path, index=False)
        
        print(f" Dataset de ejemplo creado: {example_path}")
        print(f"    {len(df)} registros con {len(df.columns)} columnas")
        
        return True
        
    except Exception as e:
        print(f" Error creando datos de ejemplo: {e}")
        return False

def print_usage_instructions():
    """Imprime instrucciones de uso"""
    print_header("INSTRUCCIONES DE USO")
    
    print(" Para ejecutar el sistema:")
    print("   python main.py")
    print()
    print(" Estructura de directorios:")
    print("   datos/          - Archivos CSV y datasets")
    print("   multimedia_data/ - Características y modelos multimedia")
    print("   indices/        - Índices construidos")
    print("   logs/           - Archivos de log")
    print()
    print("📖 Funcionalidades disponibles:")
    print("   1. Índices tradicionales (Sequential, Hash, B-Tree, ISAM, R-Tree)")
    print("   2. Búsqueda textual con SPIMI")
    print("   3. Búsqueda multimedia por similitud")
    print("   4. Comparación de rendimiento")
    print()
    print(" Consejos:")
    print("   - Usa datasets pequeños para pruebas iniciales")
    print("   - Verifica que los archivos multimedia existan antes de procesarlos")
    print("   - Las dependencias opcionales mejoran la funcionalidad pero no son requeridas")

def main():
    """Función principal de configuración"""
    print(" Sistema de Base de Datos Multimodal - Configuración")
    print("Proyecto 2 - BDII")
    
    # Verificar Python
    if not check_python_version():
        sys.exit(1)
    
    # Crear directorios
    create_directories()
    
    # Instalar dependencias
    if not install_core_dependencies():
        print(" Error instalando dependencias básicas")
        sys.exit(1)
    
    # Dependencias opcionales
    install_optional_dependencies()
    
    # Configurar NLTK
    download_nltk_data()
    
    # Verificar instalación
    verify_installation()
    
    # Crear datos de ejemplo
    create_example_data()
    
    # Probar sistema
    if test_system():
        print_usage_instructions()
        print("\n🎉 ¡Configuración completada exitosamente!")
    else:
        print("\n La configuración tuvo algunos problemas. Revisa los errores arriba.")
        sys.exit(1)

if __name__ == "__main__":
    main()