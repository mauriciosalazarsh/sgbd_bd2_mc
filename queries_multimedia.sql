-- ============================================================
-- CONSULTAS DE EJEMPLO - SISTEMA DE BASE DE DATOS MULTIMEDIA
-- ============================================================

-- 1. CREAR TABLA CON DATOS DE SPOTIFY (Índice de texto SPIMI)
CREATE TABLE spotify_songs FROM FILE "datos/spotify_songs.csv" USING INDEX spimi("track_name", "track_artist", "lyrics")

-- 2. CREAR TABLA DE MODA (Para búsqueda multimedia futura)
CREATE TABLE fashion_items FROM FILE "datos/fashion_complete_dataset.csv" USING INDEX hash(0)

-- ============================================================
-- BÚSQUEDAS DE TEXTO EN CANCIONES
-- ============================================================

-- Buscar canciones que contengan "love" en las letras
SELECT * FROM spotify_songs WHERE lyrics @@ 'love' LIMIT 10

-- Buscar canciones con "dance" en cualquier campo de texto
SELECT * FROM spotify_songs WHERE track_name @@ 'dance' LIMIT 10

-- Buscar por artista
SELECT * FROM spotify_songs WHERE track_artist @@ 'Taylor Swift' LIMIT 10

-- Buscar canciones románticas
SELECT * FROM spotify_songs WHERE lyrics @@ 'romantic heart' LIMIT 10

-- Buscar canciones en español
SELECT * FROM spotify_songs WHERE lyrics @@ 'amor corazon' LIMIT 10

-- ============================================================
-- BÚSQUEDAS TRADICIONALES
-- ============================================================

-- Buscar por ID específico (si tienes tabla con hash)
SELECT * FROM fashion_items WHERE id = 1000

-- Buscar rango de IDs (si tienes tabla con btree)
SELECT * FROM fashion_items WHERE id BETWEEN 1000 AND 1010

-- ============================================================
-- BÚSQUEDAS COMPLEJAS DE TEXTO
-- ============================================================

-- Buscar canciones felices
SELECT * FROM spotify_songs WHERE lyrics @@ 'happy joy fun' LIMIT 10

-- Buscar canciones tristes
SELECT * FROM spotify_songs WHERE lyrics @@ 'sad tears cry' LIMIT 10

-- Buscar por género implícito
SELECT * FROM spotify_songs WHERE lyrics @@ 'rock guitar drums' LIMIT 10

-- Buscar canciones de fiesta
SELECT * FROM spotify_songs WHERE lyrics @@ 'party dance night' LIMIT 10

-- ============================================================
-- CONSULTAS DE INFORMACIÓN
-- ============================================================

-- Ver todas las tablas
SHOW TABLES

-- Ver estructura de una tabla
DESCRIBE spotify_songs

-- ============================================================
-- CONSULTAS MULTIMEDIA (PRÓXIMAMENTE)
-- ============================================================

-- Crear índice multimedia para imágenes
-- CREATE MULTIMEDIA INDEX ON fashion_items USING sift FOR images WITH PATH_COLUMN "image_path"

-- Buscar imágenes similares
-- SELECT * FROM fashion_items WHERE image_similarity('query_image.jpg') > 0.8 LIMIT 10

-- Buscar por características visuales
-- SELECT * FROM fashion_items ORDER BY image_similarity('reference.jpg') DESC LIMIT 20