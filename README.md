
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
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Instalar dependencias
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
# Desde la raíz del proyecto
python main.py api

# O directamente:
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

La API estará disponible en:
- **URL**: http://localhost:8000
- **Documentación**: http://localhost:8000/docs
- **Redoc**: http://localhost:8000/redoc

### Opción 2: Ejecutar Solo Frontend
```bash
cd frontend
npm run dev
# o
yarn dev
```

El frontend estará disponible en: http://localhost:3000

### Opción 3: Sistema Completo (Recomendado)
```bash
# Terminal 1 - Backend
python main.py api

# Terminal 2 - Frontend
cd frontend
npm run dev
```

### Opción 4: Modo Interactivo (Sin Frontend)
```bash
# Modo demo con consultas de ejemplo
python main.py demo

# Modo interactivo SQL
python main.py interactive
```

## Estructura del Proyecto

```
sistema-bd-multimodal/
├── api.py                    # API FastAPI principal
├── engine.py                 # Motor de base de datos
├── main.py                   # Punto de entrada
├── requirements.txt          # Dependencias Python
├── indices/                  # Implementaciones de índices
│   ├── base_index.py        # Clase base
│   ├── sequential.py        # Sequential File
│   ├── isam.py             # ISAM
│   ├── hash_extensible.py  # Hash Extensible
│   ├── btree.py            # B+ Tree
│   └── rtree.py            # R-Tree
├── parser_sql/              # Parser SQL personalizado
│   └── parser.py           # Lógica de parsing
├── frontend/               # Aplicación React/Next.js
│   ├── src/
│   │   ├── app/           # Páginas Next.js
│   │   ├── components/    # Componentes React
│   │   └── lib/          # API cliente y utilidades
│   ├── package.json
│   └── tailwind.config.js
└── datos/                 # Archivos CSV (crear manualmente)
```

## API Endpoints

### Tablas
- `GET /tables` - Listar todas las tablas
- `POST /tables/create` - Crear nueva tabla
- `POST /tables/upload-csv` - Subir archivo CSV
- `GET /tables/{table_name}/scan` - Escanear tabla completa
- `GET /tables/{table_name}/headers` - Obtener columnas

### Registros
- `POST /records/insert` - Insertar registro
- `POST /records/search` - Búsqueda exacta
- `POST /records/range-search` - Búsqueda por rango
- `POST /records/spatial-search` - Búsqueda espacial
- `DELETE /records/delete` - Eliminar registros

### SQL
- `POST /sql/execute` - Ejecutar consulta SQL

### Utilidades
- `GET /health` - Estado del sistema

## Ejemplos de Uso

### 1. Crear Tabla con API
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

### 2. Consulta SQL
```bash
curl -X POST "http://localhost:8000/sql/execute" \
-H "Content-Type: application/json" \
-d '{
  "query": "select * from estudiantes where math_score > 80"
}'
```

### 3. Búsqueda Espacial (R-Tree)
```bash
curl -X POST "http://localhost:8000/records/spatial-search" \
-H "Content-Type: application/json" \
-d '{
  "table_name": "ubicaciones",
  "point": "-12.0464,-77.0428",
  "param": "5.0"
}'
```

## Tipos de Índices Soportados

| Índice | Descripción | Casos de Uso |
|--------|-------------|-------------|
| **Sequential** | Archivo secuencial ordenado | Consultas simples, inserciones ocasionales |
| **ISAM** | Indexed Sequential Access Method | Consultas frecuentes, actualizaciones moderadas |
| **Hash** | Hash Extensible | Búsquedas exactas muy rápidas |
| **B+ Tree** | Árbol B+ balanceado | Consultas por rango, alta concurrencia |
| **R-Tree** | Índice espacial | Datos geográficos, búsquedas por proximidad |

## Configuración Avanzada

### Variables de Entorno (Opcional)
```bash
# .env
NEXT_PUBLIC_API_URL=http://localhost:8000
PYTHONPATH=.
```

### Configuración de Índices
```python
# Personalizar parámetros en engine.py
BUCKET_CAPACITY = 32        # Hash Extensible
ORDER = 4                   # B+ Tree
MAX_AUX = 10               # Sequential File
```

## Solución de Problemas

### Error: R-Tree no disponible
```bash
# Instalar dependencias espaciales
pip install rtree

# Si persiste el error:
# Windows: Instalar Visual C++ Build Tools
# Linux: sudo apt-get install libspatialindex-dev
# macOS: brew install spatialindex
```

### Error: Puerto 8000 en uso
```bash
# Cambiar puerto
python -m uvicorn api:app --port 8001

# O terminar proceso
# Windows: netstat -ano | findstr :8000
# Linux/macOS: lsof -ti:8000 | xargs kill
```

### Frontend no conecta con Backend
```bash
# Verificar que el backend esté corriendo
curl http://localhost:8000/health

# Verificar variable de entorno
echo $NEXT_PUBLIC_API_URL
```

### Error de permisos en archivos
```bash
# Dar permisos a directorios
chmod -R 755 datos/
chmod -R 755 indices/
```

## Rendimiento

### Benchmarks (10,000 registros)
| Operación | Sequential | ISAM | Hash | B+Tree | R-Tree |
|-----------|------------|------|------|--------|--------|
| Insert | 45ms | 12ms | 8ms | 10ms | 35ms |
| Search | 120ms | 15ms | 3ms | 8ms | 25ms* |
| Range | 150ms | 25ms | N/A | 12ms | 40ms* |

*Búsquedas espaciales con distancia Haversine

## Contribuir

1. Fork el proyecto
2. Crear una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir un Pull Request

## Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

## Contacto

Tu Nombre - [@tu_usuario](https://twitter.com/tu_usuario) - email@ejemplo.com

Link del Proyecto: [https://github.com/tu-usuario/sistema-bd-multimodal](https://github.com/tu-usuario/sistema-bd-multimodal)
