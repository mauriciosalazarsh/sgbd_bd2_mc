# Sistema de Base de Datos Multimedia

Este proyecto implementa un sistema de base de datos con capacidades de búsqueda multimedia que soporta consultas sobre texto, imágenes y audio mediante SQL extendido.

## Requisitos del Sistema

- Python 3.10+
- PostgreSQL (opcional para comparación)
- Dependencias: `pip install -r requirements.txt`

## Instalación y Configuración

### 1. Clonar el repositorio
```bash
git clone https://github.com/mauriciosalazarsh/sgbd_bd2_mc.git
cd Proyecto1
```

### 2. Instalar dependencias
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Preparar los datos
```bash
# Crear datasets de prueba
python create_fashion_dataset.py
python create_fma_dataset.py
```

### 4. Iniciar el servidor API
```bash
cd backend
python api.py
```

### 5. Iniciar el frontend (opcional)
```bash
cd frontend
npm install
npm run dev
```

## Uso del Sistema

### Búsqueda de Texto (SPIMI)

El sistema utiliza el algoritmo SPIMI (Single Pass In-Memory Indexing) para indexar y buscar en campos de texto.

#### Crear índice de texto
```sql
CREATE TABLE spotify_songs FROM FILE "datos/spotify_songs.csv" 
USING INDEX spimi("track_name", "track_artist", "lyrics")
```

#### Consultas de texto
```sql
-- Buscar canciones con "love" en las letras
SELECT * FROM spotify_songs WHERE lyrics @@ 'love' LIMIT 10

-- Buscar por artista
SELECT * FROM spotify_songs WHERE track_artist @@ 'Taylor Swift' LIMIT 10

-- Buscar canciones románticas
SELECT * FROM spotify_songs WHERE lyrics @@ 'romantic heart' LIMIT 10

-- Buscar canciones en español
SELECT * FROM spotify_songs WHERE lyrics @@ 'amor corazon' LIMIT 10
```

### Búsqueda de Imágenes

El sistema soporta tres métodos de extracción de características visuales:

#### 1. SIFT (Scale-Invariant Feature Transform)
- Descriptores locales de 128 dimensiones
- Rápido, robusto a cambios de escala y rotación
- Ideal para objetos específicos

```sql
-- Crear tabla con SIFT
CREATE MULTIMEDIA TABLE fashion_sift 
FROM FILE "datos/fashion_complete_dataset.csv" 
USING image WITH METHOD sift CLUSTERS 256
```

#### 2. ResNet50
- Red neuronal convolucional preentrenada
- Vectores de características de 2048 dimensiones
- Balance entre velocidad y precisión

```sql
-- Crear tabla con ResNet50
CREATE MULTIMEDIA TABLE fashion_resnet 
FROM FILE "datos/fashion_complete_dataset.csv" 
USING image WITH METHOD resnet50 CLUSTERS 512
```

#### 3. InceptionV3
- Red neuronal con módulos inception
- Vectores de características de 2048 dimensiones
- Alta precisión para categorización

```sql
-- Crear tabla con InceptionV3
CREATE MULTIMEDIA TABLE fashion_inception 
FROM FILE "datos/fashion_complete_dataset.csv" 
USING image WITH METHOD inception_v3 CLUSTERS 512
```

#### Consultas de imágenes
```sql
-- Búsqueda con KNN invertido (más rápido)
SELECT productDisplayName, baseColour, similarity 
FROM fashion_sift 
WHERE image_sim <-> "datos/fashion-dataset/images/1163.jpg" 
METHOD inverted LIMIT 10

-- Búsqueda con KNN secuencial (más preciso)
SELECT productDisplayName, articleType, gender 
FROM fashion_sift 
WHERE image_sim <-> "datos/fashion-dataset/images/10000.jpg" 
LIMIT 5

-- Búsqueda con ResNet50
SELECT * FROM fashion_resnet 
WHERE image_sim <-> "datos/fashion-dataset/images/7890.jpg" 
METHOD inverted LIMIT 15

-- Búsqueda con InceptionV3
SELECT productDisplayName, masterCategory, subCategory 
FROM fashion_inception 
WHERE image_sim <-> "datos/fashion-dataset/images/3456.jpg" 
LIMIT 8
```

### Búsqueda de Audio

El sistema soporta tres métodos de extracción de características de audio:

#### 1. MFCC (Mel-frequency Cepstral Coefficients)
- 13 coeficientes cepstrales + estadísticas
- Estándar para análisis de voz y música

#### 2. Espectrograma
- Características del espectrograma de Mel (128 bandas)
- Captura información temporal y frecuencial

#### 3. Características Comprehensivas
- Combina MFCC, centroide espectral, rolloff, zero-crossing rate
- Más completo pero más lento

```sql
-- Crear tabla de audio con MFCC
CREATE MULTIMEDIA TABLE fma_audio 
FROM FILE "datos/fma_subset_2000.csv" 
USING audio WITH METHOD mfcc CLUSTERS 256
```

#### Consultas de audio
```sql
-- Búsqueda con KNN invertido
SELECT title, artist, genre 
FROM fma_audio 
WHERE audio_sim <-> "datos/fma_medium/000/000002.mp3" 
METHOD inverted LIMIT 10

-- Búsqueda con KNN secuencial
SELECT title, artist, album 
FROM fma_audio 
WHERE audio_sim <-> "datos/fma_medium/003/003456.mp3" 
LIMIT 20

-- Más ejemplos
SELECT title, genre, year 
FROM fma_audio 
WHERE audio_sim <-> "datos/fma_medium/005/005123.mp3" 
METHOD inverted LIMIT 8

SELECT title, artist, duration_seconds 
FROM fma_audio 
WHERE audio_sim <-> "datos/fma_medium/020/020567.mp3" 
LIMIT 7
```

## Métodos de Búsqueda

### KNN Secuencial
- Escanea todos los vectores de la base de datos
- Calcula similitud coseno con cada vector
- Complejidad: O(n·d) donde n = documentos, d = dimensiones
- Más preciso pero más lento

### KNN Invertido
- Utiliza índice invertido: palabra_visual → [(doc_id, peso)]
- Solo evalúa documentos que comparten palabras visuales/auditivas
- Aplica ponderación TF-IDF
- Significativamente más rápido para vectores dispersos

## Estructura del Proyecto

```
Proyecto1/
├── backend/
│   └── api.py              # API REST del sistema
├── frontend/               # Interfaz web React
├── indices/               # Implementación de índices
│   ├── spimi.py          # Índice de texto SPIMI
│   ├── hash_extensible.py # Hash extensible
│   ├── btree.py         # Árbol B+
│   └── rtree.py         # Árbol R
├── multimedia/           # Módulos multimedia
│   ├── feature_extractors/
│   │   ├── image_extractor.py  # SIFT, ResNet, Inception
│   │   └── audio_extractor.py  # MFCC, Espectrograma
│   ├── search/
│   │   ├── knn_sequential.py   # KNN secuencial
│   │   └── knn_inverted.py     # KNN con índice invertido
│   └── codebook/         # Generación de vocabulario visual
├── parser_sql/
│   └── parser.py         # Parser SQL extendido
└── datos/               # Datasets
```

## API Endpoints

- `POST /search/text` - Búsqueda de texto
- `POST /multimedia/search` - Búsqueda multimedia
- `POST /multimedia/create-table` - Crear tabla multimedia
- `POST /multimedia/benchmark` - Comparar métodos de búsqueda

## Consideraciones de Rendimiento

1. **Tamaño del Codebook**: Más clusters mejoran precisión pero aumentan tiempo de procesamiento
2. **Método de Extracción**: SIFT es rápido, ResNet/Inception más precisos
3. **Método de Búsqueda**: KNN invertido es 5-10x más rápido que secuencial
4. **Preprocesamiento**: La extracción de características puede tomar tiempo considerable

## Ejemplos de Uso Completo

### Flujo típico para búsqueda de imágenes
```sql
-- 1. Crear tabla multimedia
CREATE MULTIMEDIA TABLE fashion 
FROM FILE "datos/fashion_demo_100.csv" 
USING image WITH METHOD sift CLUSTERS 64

-- 2. Realizar búsquedas
SELECT productDisplayName, baseColour, similarity 
FROM fashion 
WHERE image_sim <-> "datos/fashion-dataset/images/1163.jpg" 
METHOD inverted LIMIT 10
```

### Flujo típico para búsqueda de audio
```sql
-- 1. Crear tabla multimedia
CREATE MULTIMEDIA TABLE music 
FROM FILE "datos/fma_demo_100.csv" 
USING audio WITH METHOD mfcc CLUSTERS 64

-- 2. Realizar búsquedas
SELECT title, artist, genre 
FROM music 
WHERE audio_sim <-> "datos/fma_medium/000/000002.mp3" 
METHOD inverted LIMIT 10
```

## Troubleshooting

### Error: "No se encontró el archivo"
- Verificar que las rutas en el CSV sean correctas
- Usar rutas absolutas si es necesario

### Error: "Memoria insuficiente"
- Reducir el número de clusters
- Usar datasets más pequeños para pruebas
- Procesar en lotes

### Búsquedas lentas
- Usar método "inverted" en lugar de secuencial
- Reducir número de clusters
- Considerar usar SIFT en lugar de CNN para imágenes
