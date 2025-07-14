# Consultas SQL de Prueba - Proyecto BD2

Este documento contiene todas las consultas SQL para testear los tres tipos de búsqueda implementados en el proyecto.

## 1. Búsqueda Textual (Text Search) - Dataset: Spotify Songs

### Crear tabla con índice textual SPIMI
```sql
CREATE TABLE spotify_songs FROM FILE "datos/spotify_songs.csv" USING INDEX spimi("track_name", "track_artist", "lyrics");
```

### Consultas de prueba
```sql
-- Buscar canciones con "love" en las letras
SELECT track_name, track_artist FROM spotify_songs WHERE lyrics @@ "love" LIMIT 10;

-- Buscar canciones con "happy dance" en las letras
SELECT track_name, track_artist, lyrics FROM spotify_songs WHERE lyrics @@ "happy dance" LIMIT 5;

-- Buscar canciones con "summer" en el título
SELECT * FROM spotify_songs WHERE track_name @@ "summer" LIMIT 15;

-- Buscar canciones del artista Drake
SELECT track_name, track_artist FROM spotify_songs WHERE track_artist @@ "Drake" LIMIT 10;

-- Buscar canciones con "night" en las letras
SELECT track_name, track_artist, track_album_name FROM spotify_songs WHERE lyrics @@ "night" LIMIT 8;

-- Buscar canciones con "party" en el título
SELECT track_name, track_artist FROM spotify_songs WHERE track_name @@ "party" LIMIT 20;

-- Buscar canciones con múltiples palabras
SELECT track_name, track_artist FROM spotify_songs WHERE lyrics @@ "dance with me" LIMIT 10;

-- Buscar por género de playlist
SELECT track_name, playlist_genre FROM spotify_songs WHERE playlist_genre @@ "pop" LIMIT 5;
```

## 2. Búsqueda de Imágenes (Image Search) - Dataset: Fashion

### Crear tablas multimedia para imágenes

#### Opción 1: Usando SIFT (más rápido, menos preciso)
```sql
CREATE MULTIMEDIA TABLE fashion_sift FROM FILE "datos/fashion_complete_dataset.csv" USING image WITH METHOD sift CLUSTERS 256;
```

#### Opción 2: Usando ResNet50 (lento, preciso)
```sql
CREATE MULTIMEDIA TABLE fashion_resnet FROM FILE "datos/fashion_complete_dataset.csv" USING image WITH METHOD resnet50 CLUSTERS 512;
```

#### Opción 3: Usando Inception v3 CNN (más lento, muy preciso)
```sql
CREATE MULTIMEDIA TABLE fashion_inception FROM FILE "datos/fashion_complete_dataset.csv" USING image WITH METHOD inception_v3 CLUSTERS 512;
```

### Consultas de prueba con SIFT
```sql
-- Buscar imágenes similares a la imagen 1163.jpg
SELECT productDisplayName, baseColour, similarity FROM fashion_sift WHERE image_sim <-> "datos/fashion-dataset/images/1163.jpg" METHOD inverted LIMIT 10;

-- Buscar productos similares a la imagen 10000.jpg
SELECT productDisplayName, articleType, gender FROM fashion_sift WHERE image_sim <-> "datos/fashion-dataset/images/10000.jpg" LIMIT 5;

-- Buscar todos los campos para productos similares
SELECT * FROM fashion_sift WHERE image_sim <-> "datos/fashion-dataset/images/1234.jpg" METHOD inverted LIMIT 20;

-- Más ejemplos con diferentes imágenes de consulta
SELECT productDisplayName, subCategory, baseColour FROM fashion_sift WHERE image_sim <-> "datos/fashion-dataset/images/5678.jpg" LIMIT 15;

SELECT productDisplayName, usage, season FROM fashion_sift WHERE image_sim <-> "datos/fashion-dataset/images/2000.jpg" METHOD inverted LIMIT 8;

SELECT productDisplayName, year, masterCategory FROM fashion_sift WHERE image_sim <-> "datos/fashion-dataset/images/3456.jpg" LIMIT 12;
```

### Consultas de prueba con ResNet50
```sql
-- Búsqueda más precisa con ResNet50
SELECT productDisplayName, baseColour FROM fashion_resnet WHERE image_sim <-> "datos/fashion-dataset/images/1163.jpg" METHOD inverted LIMIT 10;

SELECT * FROM fashion_resnet WHERE image_sim <-> "datos/fashion-dataset/images/7890.jpg" LIMIT 15;

SELECT productDisplayName, articleType, gender FROM fashion_resnet WHERE image_sim <-> "datos/fashion-dataset/images/2500.jpg" METHOD inverted LIMIT 12;
```

### Consultas de prueba con Inception v3 CNN
```sql
-- Búsqueda de alta precisión con Inception v3
SELECT productDisplayName, baseColour FROM fashion_inception WHERE image_sim <-> "datos/fashion-dataset/images/1163.jpg" METHOD inverted LIMIT 10;

SELECT productDisplayName, articleType, gender FROM fashion_inception WHERE image_sim <-> "datos/fashion-dataset/images/10000.jpg" LIMIT 15;

SELECT * FROM fashion_inception WHERE image_sim <-> "datos/fashion-dataset/images/5678.jpg" METHOD inverted LIMIT 20;

-- Comparar precisión entre métodos usando la misma imagen
SELECT productDisplayName, masterCategory, subCategory FROM fashion_inception WHERE image_sim <-> "datos/fashion-dataset/images/3456.jpg" LIMIT 8;

SELECT productDisplayName, baseColour, usage FROM fashion_inception WHERE image_sim <-> "datos/fashion-dataset/images/7890.jpg" METHOD inverted LIMIT 12;
```

## 3. Búsqueda de Audio (Audio Search) - Dataset: FMA (Free Music Archive)

### Crear tabla multimedia para audio
```sql
CREATE MULTIMEDIA TABLE fma_audio FROM FILE "datos/fma_complete_dataset.csv" USING audio WITH METHOD mfcc CLUSTERS 256;
```

### Consultas de prueba
```sql
-- Buscar canciones similares a 000002.mp3
SELECT title, artist, genre FROM fma_audio WHERE audio_sim <-> "datos/fma_medium/000/000002.mp3" METHOD inverted LIMIT 10;

-- Buscar canciones similares con duración
SELECT title, artist, duration FROM fma_audio WHERE audio_sim <-> "datos/fma_medium/001/001486.mp3" LIMIT 15;

-- Buscar todas las columnas para canciones similares
SELECT * FROM fma_audio WHERE audio_sim <-> "datos/fma_medium/002/002819.mp3" METHOD inverted LIMIT 5;

-- Más ejemplos con diferentes archivos de audio
SELECT title, artist, album FROM fma_audio WHERE audio_sim <-> "datos/fma_medium/003/003456.mp3" LIMIT 20;

SELECT title, genre, year FROM fma_audio WHERE audio_sim <-> "datos/fma_medium/005/005123.mp3" METHOD inverted LIMIT 8;

SELECT title, artist, file_size_mb FROM fma_audio WHERE audio_sim <-> "datos/fma_medium/010/010789.mp3" LIMIT 12;

-- Búsqueda con archivos de diferentes carpetas
SELECT title, artist FROM fma_audio WHERE audio_sim <-> "datos/fma_medium/015/015234.mp3" METHOD inverted LIMIT 10;

SELECT title, genre, duration_seconds FROM fma_audio WHERE audio_sim <-> "datos/fma_medium/020/020567.mp3" LIMIT 7;
```

## Notas Importantes para el Testing

### Sintaxis General:
- **Text Search**: `SELECT ... FROM tabla WHERE campo @@ "consulta" LIMIT k;`
- **Image/Audio Search**: `SELECT ... FROM tabla WHERE campo_sim <-> "ruta_archivo" [METHOD método] LIMIT k;`

### Parámetros:
- **LIMIT**: Número de resultados a retornar (por defecto 10)
- **METHOD**: Para búsquedas multimedia, por defecto es "inverted"
- **CLUSTERS**: En la creación de tablas multimedia, define el tamaño del vocabulario (256, 512, etc.)

### Tiempos de Ejecución Esperados:
1. **Creación de índices**:
   - Text (SPIMI): 10-30 segundos
   - Image (SIFT): 1-3 minutos
   - Image (ResNet50): 5-10 minutos
   - **Image (Inception v3): 8-15 minutos** (el más lento pero más preciso)
   - Audio (MFCC): 3-5 minutos

2. **Búsquedas**:
   - Text: < 1 segundo
   - Image/Audio: 1-3 segundos

3. **Comparación de métodos de imagen**:
   - **SIFT**: Rápido de construir, menos preciso, bueno para demostraciones
   - **ResNet50**: Balance entre velocidad y precisión, CNN moderno
   - **Inception v3**: Máxima precisión, CNN avanzado de Google, mejor para evaluación final

### Verificación de Archivos:
Antes de ejecutar las consultas, verifica que existan los archivos de consulta:
- Para imágenes: archivos en `datos/fashion-dataset/images/`
- Para audio: archivos en `datos/fma_medium/XXX/`

### Orden Recomendado de Testing:
1. Primero crear todas las tablas
2. Ejecutar consultas de búsqueda textual (más rápidas)
3. Ejecutar consultas de búsqueda de imágenes
4. Ejecutar consultas de búsqueda de audio

### Debugging:
Si alguna consulta falla, verificar:
- Que el archivo CSV existe y tiene las columnas esperadas
- Que los archivos de consulta (imágenes/audio) existen
- Que se creó la tabla correctamente antes de hacer búsquedas
- Los logs del sistema para más detalles del error