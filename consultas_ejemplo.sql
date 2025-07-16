-- Ejemplos de consultas SQL para el Sistema de Base de Datos Multimedia

-- 1. Crear tabla con índice Hash
CREATE TABLE productos FROM FILE "datos/productos.csv" USING INDEX hash(0)

-- 2. Crear tabla con índice B+Tree
CREATE TABLE empleados FROM FILE "datos/empleados.csv" USING INDEX btree(1)

-- 3. Crear tabla con índice R-Tree (para datos geoespaciales)
CREATE TABLE ubicaciones FROM FILE "datos/ubicaciones.csv" USING INDEX rtree

-- 4. Crear tabla con índice de texto SPIMI
CREATE TABLE canciones FROM FILE "datos/spotify_songs.csv" USING INDEX spimi("track_name", "track_artist", "lyrics")

-- 5. Búsquedas simples
SELECT * FROM productos WHERE id = 100
SELECT * FROM empleados WHERE salario BETWEEN 50000 AND 100000

-- 6. Búsquedas de texto
SELECT * FROM canciones WHERE lyrics @@ 'love'
SELECT * FROM canciones WHERE track_name @@ 'hello' LIMIT 10

-- 7. Búsquedas espaciales
SELECT * FROM ubicaciones WHERE location <-> '(40.7128, -74.0060)' < 5
SELECT * FROM ubicaciones ORDER BY location <-> '(40.7128, -74.0060)' LIMIT 10
