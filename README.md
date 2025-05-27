# Sistema de Base de Datos Multimodal

[![Python](https://img.shields.io/badge/python-v3.8+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com/)
[![Next.js](https://img.shields.io/badge/Next.js-14+-black.svg)](https://nextjs.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-blue.svg)](https://www.typescriptlang.org/)

Un sistema de base de datos personalizado que implementa múltiples estructuras de indexación con una interfaz web moderna y API REST completa.

## Características

- **5 Tipos de Índices**: Sequential File, ISAM, Hash Extensible, B+ Tree, R-Tree
- **Parser SQL**: Soporte para CREATE, SELECT, INSERT, DELETE
- **API REST**: Backend FastAPI con documentación automática
- **Frontend Moderno**: React/Next.js con TypeScript
- **Búsquedas Espaciales**: R-Tree con distancia Haversine
- **Generación de Datos**: Datasets sintéticos masivos
- **Persistencia**: Almacenamiento en memoria secundaria

## Requisitos del Sistema

### Backend
```bash
Python 3.8+
pip (administrador de paquetes de Python)
```

### Frontend
```bash
Node.js 18+
npm o yarn
```

### Dependencias Espaciales (Opcional - para R-Tree)
- **Windows**: Instalar [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
- **Linux**: `sudo apt-get install libspatialindex-dev`
- **macOS**: `brew install spatialindex`

## Instalación

### 1. Clonar el Repositorio
```bash
git clone https://github.com/tu-usuario/sistema-bd-multimodal.git
cd sistema-bd-multimodal
```

### 2. Configurar Backend
```bash
python -m venv venv
# Activar entorno virtual
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configurar Frontend
```bash
cd frontend
npm install
# o
yarn install
```

### 4. Crear Directorios de Datos
```bash
mkdir datos
mkdir indices
mkdir indices/buckets
```

## Ejecución

### Opción 1: Ejecutar Solo Backend (API)
```bash
python main.py api
# o directamente:
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

API disponible en:
- http://localhost:8000
- http://localhost:8000/docs
- http://localhost:8000/redoc

### Opción 2: Ejecutar Solo Frontend
```bash
cd frontend
npm run dev
# o
yarn dev
```

Frontend en: http://localhost:3000

### Opción 3: Sistema Completo (Recomendado)
```bash
# Terminal 1
python main.py api
# Terminal 2
cd frontend
npm run dev
```

### Opción 4: Modo Interactivo (Demo o SQL directo)
```bash
python main.py demo
python main.py interactive
```

## Estructura del Proyecto

```
sistema-bd-multimodal/
├── api.py
├── engine.py
├── main.py
├── requirements.txt
├── indices/
│   ├── base_index.py
│   ├── sequential.py
│   ├── isam.py
│   ├── hash_extensible.py
│   ├── btree.py
│   └── rtree.py
├── parser_sql/
│   └── parser.py
├── frontend/
│   ├── src/
│   │   ├── app/
│   │   ├── components/
│   │   └── lib/
│   ├── package.json
│   └── tailwind.config.js
└── datos/
```

## API Endpoints

### Tablas
- `GET /tables`
- `POST /tables/create`
- `POST /tables/upload-csv`
- `GET /tables/{table_name}/scan`
- `GET /tables/{table_name}/headers`

### Registros
- `POST /records/insert`
- `POST /records/search`
- `POST /records/range-search`
- `POST /records/spatial-search`
- `DELETE /records/delete`

### SQL
- `POST /sql/execute`

### Utilidades
- `GET /health`

## Ejemplos de Uso

### Crear Tabla (API)
```bash
curl -X POST "http://localhost:8000/tables/create" \
-H "Content-Type: application/json" \
-d '{
  "table_name": "estudiantes",
  "csv_file_path": "datos/students.csv",
  "index_type": "btree",
  "index_field": 0
}'
```

### Consulta SQL (API)
```bash
curl -X POST "http://localhost:8000/sql/execute" \
-H "Content-Type: application/json" \
-d '{
  "query": "select * from estudiantes where math_score > 80"
}'
```

### Búsqueda Espacial (API)
```bash
curl -X POST "http://localhost:8000/records/spatial-search" \
-H "Content-Type: application/json" \
-d '{
  "table_name": "ubicaciones",
  "point": "-12.0464,-77.0428",
  "param": "5.0"
}'
```

## Consultas SQL por Tipo de Índice

### 🔹 SEQUENTIAL
```sql
CREATE TABLE students_seq FROM FILE "datos/StudentsPerformance.csv" USING INDEX sequential("5");
INSERT INTO students_seq GENERATE_DATA(100);
SELECT * FROM students_seq WHERE math_score = 70;
SELECT * FROM students_seq WHERE math_score BETWEEN 70 AND 90;
DELETE FROM students_seq WHERE math_score = 70;
```

### 🔹 ISAM
```sql
CREATE TABLE students_isam FROM FILE "datos/StudentsPerformance.csv" USING INDEX isam("5");
INSERT INTO students_isam GENERATE_DATA(100);
SELECT * FROM students_isam WHERE math_score = 70;
SELECT * FROM students_isam WHERE math_score BETWEEN 70 AND 90;
DELETE FROM students_isam WHERE math_score = 70;
```

### 🔹 B+ TREE
```sql
CREATE TABLE students_btree FROM FILE "datos/StudentsPerformance.csv" USING INDEX btree("5");
INSERT INTO students_btree GENERATE_DATA(100);
SELECT * FROM students_btree WHERE math_score = 70;
SELECT * FROM students_btree WHERE math_score BETWEEN 70 AND 90;
DELETE FROM students_btree WHERE math_score = 70;
```

### 🔹 HASH EXTENSIBLE
```sql
CREATE TABLE students_hash FROM FILE "datos/StudentsPerformance.csv" USING INDEX hash("5");
INSERT INTO students_hash GENERATE_DATA(100);
SELECT * FROM students_hash WHERE math_score = 70;
-- ❌ NO SOPORTA búsqueda por rango
DELETE FROM students_hash WHERE math_score = 70;
```

### 🔹 R-TREE
```sql
CREATE TABLE houses_rtree FROM FILE "datos/kcdatahouse.csv" USING INDEX rtree("lat");
INSERT INTO houses_rtree GENERATE_DATA(100);
SELECT * FROM houses_rtree WHERE lat IN ("47.6, -122.3", 10);
SELECT * FROM houses_rtree WHERE lat IN ("47.6, -122.3", 5.0);
```

## Tipos de Índices Soportados

| Índice         | Descripción                        | Casos de Uso                        |
|----------------|------------------------------------|-------------------------------------|
| Sequential     | Archivo secuencial ordenado        | Consultas simples, inserciones bajas|
| ISAM           | Índice secuencial con acceso mixto | Lectura rápida y actualizaciones    |
| Hash Extensible| Hash dinámico en disco            | Búsqueda exacta muy rápida          |
| B+ Tree        | Árbol balanceado con ordenamiento | Consultas por rango, escalabilidad  |
| R-Tree         | Índice espacial                    | Datos geográficos y proximidad      |

## Solución de Problemas

### Error: R-Tree no disponible
```bash
pip install rtree
# Windows: instalar Visual C++ Build Tools
# Linux: sudo apt-get install libspatialindex-dev
# macOS: brew install spatialindex
```

### Puerto 8000 ocupado
```bash
python -m uvicorn api:app --port 8001
# o terminar proceso:
# Windows: netstat -ano | findstr :8000
# Linux/macOS: lsof -ti:8000 | xargs kill
```

### Frontend no conecta con Backend
```bash
curl http://localhost:8000/health
echo $NEXT_PUBLIC_API_URL
```

### Permisos en archivos
```bash
chmod -R 755 datos/
chmod -R 755 indices/
```

## Contribuir

1. Fork del repositorio  
2. Crea una rama (`git checkout -b feature/NuevaFeature`)  
3. Haz commit (`git commit -m 'Add NuevaFeature'`)  
4. Push (`git push origin feature/NuevaFeature`)  
5. Abre un Pull Request
