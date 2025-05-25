# 🚀 Sistema de Base de Datos Multimodal con Indexación Avanzada

## 📋 Descripción General

Sistema de base de datos multimodal desarrollado en Python que integra **5 técnicas de indexación avanzadas** para optimizar operaciones CRUD en diferentes tipos de datos. Incluye un **parser SQL personalizado** y una **API REST completa** para integración con aplicaciones frontend.

### 🎯 Características Principales

- ✅ **5 Índices Implementados**: Sequential File, ISAM, Extendible Hashing, B+ Tree, R-Tree
- ✅ **Parser SQL Personalizado**: Sintaxis tipo SQL con extensiones espaciales
- ✅ **API REST FastAPI**: 15+ endpoints con documentación automática
- ✅ **Consultas Espaciales**: Búsquedas por radio y K-vecinos más cercanos
- ✅ **Operaciones CRUD Completas**: Create, Read, Update, Delete
- ✅ **Carga de Datos CSV**: Import masivo desde archivos planos
- ✅ **Múltiples Tipos de Búsqueda**: Exacta, por rango, espacial

---

## 📁 Estructura del Proyecto

```
Proyecto1/
├── 📁 backend/
│   └── api.py                  # API FastAPI completa
├── 📁 datos/                   # Archivos CSV de prueba
│   ├── StudentsPerformance.csv
│   ├── powerplants.csv
│   └── kcdatahouse.csv
├── 📁 indices/                 # Implementaciones de índices
│   ├── base_index.py          # Clase abstracta base
│   ├── sequential.py          # Sequential File + Auxiliary Space
│   ├── isam.py               # ISAM de 2 niveles + Overflow
│   ├── hash_extensible.py    # Extendible Hashing dinámico
│   ├── btree.py             # B+ Tree con enlaces entre hojas
│   └── rtree.py             # R-Tree para datos espaciales
├── 📁 parser_sql/
│   └── parser.py            # Parser SQL personalizado
├── 📁 gui/
│   └── interfaz.py          # Interfaz gráfica (opcional)
├── main.py                  # Punto de entrada principal
├── engine.py               # Motor de base de datos
├── test_api.py            # Scripts de prueba
└── requirements.txt       # Dependencias del proyecto
```

---

## 🛠️ Instalación y Configuración

### **Requisitos Previos**
- Python 3.8+
- pip package manager
- 4GB RAM mínimo
- 1GB espacio en disco

### **1. Clonar Repositorio**
```bash
git clone <tu-repositorio>
cd Proyecto1
```

### **2. Crear Entorno Virtual**
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

### **3. Instalar Dependencias**
```bash
pip install -r requirements.txt
```

### **4. Verificar Instalación**
```bash
python test_api.py
# Seleccionar opción 2 (Parser SQL)
```

---

## 🎯 Modos de Uso

### **1. Modo Interactivo SQL** 💬
```bash
python main.py interactive
```
**Ideal para:** Pruebas rápidas, desarrollo, aprendizaje

### **2. API FastAPI** 🌐
```bash
python main.py api
# Acceder a: http://localhost:8000/docs
```
**Ideal para:** Aplicaciones web, integración con frontend

### **3. Demo Automático** 🎪
```bash
python main.py demo
```
**Ideal para:** Presentaciones, validación rápida

### **4. Testing Completo** 🧪
```bash
python test_api.py
```
**Ideal para:** Validación de funcionalidades, debugging

---

## 📊 Índices Implementados

### **1. Sequential File** 📝
**Características:**
- Archivo secuencial ordenado por clave
- Espacio auxiliar para nuevas inserciones
- Reconstrucción automática cuando aux > K registros

**Ideal para:**
- Datos ordenados naturalmente
- Acceso secuencial frecuente
- Datasets pequeños-medianos

**Ejemplo:**
```sql
create table products from file "datos/products.csv" using index sequential("product_id")
select * from products where product_id = "P001"
select * from products where product_id between "P001" and "P100"
```

### **2. ISAM (Indexed Sequential Access Method)** 🏗️
**Características:**
- Índice estático de 2 niveles
- Páginas de overflow para nuevas inserciones
- Factor de bloque configurable

**Ideal para:**
- Datos históricos (pocas actualizaciones)
- Consultas por rango frecuentes
- Datasets medianos

**Ejemplo:**
```sql
create table customers from file "datos/customers.csv" using index isam("customer_id")
select * from customers where customer_id between "C001" and "C500"
```

### **3. Extendible Hashing** #️⃣
**Características:**
- Hash dinámico con directorio extensible
- Buckets con capacidad configurable
- Manejo automático de overflow

**Ideal para:**
- Búsquedas exactas muy frecuentes
- Inserciones/eliminaciones dinámicas
- Distribución uniforme de claves

**Ejemplo:**
```sql
create table orders from file "datos/orders.csv" using index hash("order_id")
select * from orders where order_id = "ORD12345"
```

### **4. B+ Tree** 🌳
**Características:**
- Árbol balanceado auto-ajustable
- Enlaces entre hojas para recorrido secuencial
- Soporte completo para rangos

**Ideal para:**
- Consultas por rango frecuentes
- Datasets grandes
- Búsquedas mixtas (exacta + rango)

**Ejemplo:**
```sql
create table students from file "datos/StudentsPerformance.csv" using index btree("math_score")
select * from students where math_score between 80 and 95
select * from students where math_score = 90
```

### **5. R-Tree** 🗺️
**Características:**
- Índice espacial multidimensional
- Búsquedas por radio y K-vecinos
- Optimizado para datos geoespaciales

**Ideal para:**
- Datos con coordenadas (lat/lon)
- Consultas espaciales
- Aplicaciones GIS

**Ejemplo:**
```sql
create table locations from file "datos/locations.csv" using index rtree("coordinates")
-- Búsqueda por radio (5km)
select * from locations where coordinates in ("40.7128,-74.0060", 5.0)
-- K-vecinos más cercanos (10)
select * from locations where coordinates in ("40.7128,-74.0060", 10)
```

---

## 🗣️ Parser SQL Personalizado

### **Sintaxis Soportada**

#### **CREATE TABLE**
```sql
create table <nombre> from file "<ruta>" using index <tipo>("<columna>")
```

**Tipos de índice:**
- `sequential` - Sequential File
- `isam` - ISAM  
- `hash` - Extendible Hashing
- `btree` - B+ Tree
- `rtree` - R-Tree

**Ejemplos:**
```sql
create table students from file "datos/StudentsPerformance.csv" using index btree("0")
create table houses from file "datos/houses.csv" using index rtree("lat")
create table products from file "datos/products.csv" using index hash("id")
```

#### **SELECT**
```sql
-- Todos los registros
select * from <tabla>

-- Búsqueda exacta
select * from <tabla> where <columna> = <valor>

-- Búsqueda por rango
select * from <tabla> where <columna> between <inicio> and <fin>

-- Búsqueda espacial (solo R-Tree)
select * from <tabla> where <columna> in ("<lat,lon>", <radio_o_k>)
```

**Ejemplos:**
```sql
select * from students
select * from students where gender = "female"
select * from students where math_score between 70 and 90
select * from locations where coordinates in ("47.6062,-122.3321", 5.0)
```

#### **INSERT**
```sql
insert into <tabla> values (<val1>, <val2>, <val3>, ...)
```

**Ejemplos:**
```sql
insert into students values ("999", "John Doe", "male", "group A", "85", "90", "88")
insert into locations values ("Seattle", "47.6062", "-122.3321", "WA")
```

#### **DELETE**
```sql
delete from <tabla> where <columna> = <valor>
```

**Ejemplos:**
```sql
delete from students where id = "999"
delete from products where category = "discontinued"
```

---

## 🌐 API REST - Endpoints

### **Base URL:** `http://localhost:8000`

### **📊 Gestión de Tablas**

#### **Crear Tabla**
```http
POST /tables/create
Content-Type: application/json

{
  "table_name": "students",
  "csv_file_path": "datos/StudentsPerformance.csv",
  "index_type": "bplustree",
  "index_field": 0
}
```

#### **Listar Tablas**
```http
GET /tables
```

#### **Información de Tabla**
```http
GET /tables/{table_name}/info
```

#### **Escanear Tabla**
```http
GET /tables/{table_name}/scan
```

### **🔍 Gestión de Registros**

#### **Buscar Registros**
```http
POST /records/search
Content-Type: application/json

{
  "table_name": "students",
  "key": "female",
  "column": 0
}
```

#### **Búsqueda por Rango**
```http
POST /records/range-search
Content-Type: application/json

{
  "table_name": "students",
  "begin_key": "80",
  "end_key": "95"
}
```

#### **Búsqueda Espacial**
```http
POST /records/spatial-search
Content-Type: application/json

{
  "table_name": "locations",
  "point": "47.6062,-122.3321",
  "param": "5.0"
}
```

#### **Insertar Registro**
```http
POST /records/insert
Content-Type: application/json

{
  "table_name": "students",
  "values": ["999", "Test Student", "male", "group A", "85", "90", "88"]
}
```

#### **Eliminar Registros**
```http
DELETE /records/delete
Content-Type: application/json

{
  "table_name": "students",
  "key": "999"
}
```

### **🗣️ Ejecución SQL**

#### **Ejecutar Consulta SQL**
```http
POST /sql/execute
Content-Type: application/json

{
  "query": "select * from students where math_score between 80 and 90"
}
```

### **🛠️ Utilidades**

#### **Estado de la API**
```http
GET /health
```

#### **Subir Archivo CSV**
```http
POST /tables/upload-csv
Content-Type: multipart/form-data

[archivo CSV]
```

---

## 📈 Casos de Uso Prácticos

### **1. Sistema de Gestión Estudiantil** 🎓

**Objetivo:** Analizar rendimiento académico por diferentes criterios

**Implementación:**
```sql
-- Crear tabla con índice en puntajes de matemática
create table students from file "datos/StudentsPerformance.csv" using index btree("math_score")

-- Estudiantes con alto rendimiento
select * from students where math_score between 90 and 100

-- Estudiantes por grupo demográfico
select * from students where race_ethnicity = "group A"

-- Insertar nuevo estudiante
insert into students values ("1001", "Alice Johnson", "female", "group B", "master's degree", "standard", "completed", "95", "92", "94")
```

### **2. Sistema de Bienes Raíces** 🏘️

**Objetivo:** Búsquedas geoespaciales de propiedades

**Implementación:**
```sql
-- Crear índice espacial
create table houses from file "datos/kcdatahouse.csv" using index rtree("lat")

-- Casas en radio de 5km de Seattle centro
select * from houses where coordinates in ("47.6062,-122.3321", 5.0)

-- 10 casas más cercanas a ubicación específica
select * from houses where coordinates in ("47.5000,-122.3000", 10)

-- Insertar nueva propiedad
insert into houses values ("999999", "2025", "5", "4", "3500", "8000", "Seattle", "47.6500", "-122.3200")
```

### **3. Sistema de Inventario** 📦

**Objetivo:** Gestión eficiente de productos por código

**Implementación:**
```sql
-- Índice hash para búsquedas exactas rápidas
create table inventory from file "datos/products.csv" using index hash("product_id")

-- Buscar producto específico
select * from inventory where product_id = "PRD001"

-- Agregar nuevo producto
insert into inventory values ("PRD999", "New Product", "Electronics", "100", "25.99")

-- Eliminar producto descontinuado
delete from inventory where product_id = "PRD999"
```

### **4. Análisis de Infraestructura Energética** ⚡

**Objetivo:** Localización y análisis de plantas de energía

**Implementación:**
```sql
-- Crear tabla con datos de plantas energéticas
create table plants from file "datos/powerplants.csv" using index rtree("latitude")

-- Plantas en área metropolitana específica
select * from plants where coordinates in ("40.7128,-74.0060", 50.0)

-- Plantas por rango de capacidad
create table plants_cap from file "datos/powerplants.csv" using index btree("capacity")
select * from plants_cap where capacity between 100 and 500
```

---

## ⚡ Análisis de Rendimiento

### **Complejidad Temporal por Operación**

| Índice | Búsqueda | Inserción | Eliminación | Rango |
|--------|----------|-----------|-------------|-------|
| Sequential | O(n) | O(n) | O(n) | O(n) |
| ISAM | O(log n) | O(1)* | O(n) | O(log n + k) |
| Hash | O(1) | O(1) | O(1) | ❌ |
| B+ Tree | O(log n) | O(log n) | O(log n) | O(log n + k) |
| R-Tree | O(log n) | O(log n) | O(log n) | O(log n + k) |

*\* Inserción en overflow*

### **Recomendaciones de Uso**

#### **Sequential File**
- ✅ Datasets pequeños (<10K registros)
- ✅ Acceso secuencial frecuente
- ✅ Pocas actualizaciones
- ❌ Búsquedas aleatorias frecuentes

#### **ISAM**
- ✅ Datos históricos
- ✅ Consultas por rango
- ✅ Datasets medianos (10K-100K)
- ❌ Inserciones frecuentes

#### **Extendible Hashing**
- ✅ Búsquedas exactas muy frecuentes
- ✅ Inserciones/eliminaciones dinámicas
- ✅ Claves bien distribuidas
- ❌ Consultas por rango

#### **B+ Tree**
- ✅ Uso general (más versátil)
- ✅ Consultas mixtas
- ✅ Datasets grandes (>100K)
- ✅ Actualizaciones frecuentes

#### **R-Tree**
- ✅ Datos geoespaciales
- ✅ Consultas espaciales
- ✅ Aplicaciones GIS/mapas
- ❌ Datos no espaciales

---

## 🧪 Testing y Validación

### **Scripts de Prueba Incluidos**

#### **1. Test Básico**
```bash
python test_api.py
# Seleccionar opción 2
```

#### **2. Test API Completo**
```bash
# Terminal 1
python main.py api

# Terminal 2
python test_api.py
# Seleccionar opción 1
```

#### **3. Test Manual Interactivo**
```bash
python main.py interactive
```

### **Casos de Prueba por Índice**

#### **Sequential File**
```sql
create table seq_test from file "datos/StudentsPerformance.csv" using index sequential("0")
select * from seq_test where gender = "female"
insert into seq_test values ("test", "data", "male", "group A", "85")
```

#### **B+ Tree**
```sql
create table btree_test from file "datos/StudentsPerformance.csv" using index btree("math_score")
select * from btree_test where math_score between 80 and 90
```

#### **R-Tree**
```sql
create table rtree_test from file "datos/kcdatahouse.csv" using index rtree("lat")
select * from rtree_test where coordinates in ("47.6062,-122.3321", 5.0)
```

---

## 🐛 Troubleshooting

### **Problemas Comunes y Soluciones**

#### **Error: "Archivo no encontrado"**
```bash
# Verificar que el archivo existe
ls -la datos/
# Usar path absoluto si es necesario
create table test from file "/ruta/completa/archivo.csv" using index btree("0")
```

#### **Error: "Tipo de índice no soportado"**
```sql
-- ❌ Incorrecto
create table test from file "data.csv" using index tree("id")

-- ✅ Correcto
create table test from file "data.csv" using index btree("id")
```

#### **Error: "Columna no encontrada"**
```sql
-- Usar índice numérico en lugar de nombre
create table test from file "data.csv" using index btree("0")
-- En lugar de
create table test from file "data.csv" using index btree("id")
```

#### **Error: API no responde**
```bash
# Verificar que la API esté corriendo
curl http://localhost:8000/health
# Si no responde, reiniciar
python main.py api
```

#### **Error: "could not convert string to float" (R-Tree)**
```sql
-- ❌ Sintaxis incorrecta
select * from locations where coordinates in (47.6062,-122.3321, 5.0)

-- ✅ Sintaxis correcta
select * from locations where coordinates in ("47.6062,-122.3321", 5.0)
```

### **Logs y Debugging**

#### **Habilitar Logs Detallados**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

#### **Verificar Estado del Sistema**
```bash
python -c "
from engine import Engine
from parser_sql.parser import SQLParser
engine = Engine()
parser = SQLParser(engine)
print('✅ Sistema funcionando correctamente')
"
```

---

## 📚 Estructura de Datos de Ejemplo

### **StudentsPerformance.csv**
```csv
gender,race/ethnicity,parental_level_of_education,lunch,test_preparation_course,math_score,reading_score,writing_score
female,group B,bachelor's degree,standard,none,72,72,74
male,group C,some college,standard,completed,69,90,88
```

### **kcdatahouse.csv (King County Housing)**
```csv
id,date,price,bedrooms,bathrooms,sqft_living,sqft_lot,floors,waterfront,view,condition,grade,sqft_above,sqft_basement,yr_built,yr_renovated,zipcode,lat,long
1,20141013T000000,221900,3,1,1180,5650,1,0,0,3,7,1180,0,1955,0,98178,47.5112,-122.257
```

### **powerplants.csv**
```csv
name,country,fuel1,capacity_mw,latitude,longitude,primary_fuel,other_fuel1
Hunterston B,United Kingdom,Nuclear,1190,55.7167,-4.9,Nuclear,
Sizewell B,United Kingdom,Nuclear,1198,52.2139,1.6208,Nuclear,
```

---

## 🔮 Extensiones Futuras

### **Funcionalidades Planificadas**
- 🔄 **Transacciones ACID**: Soporte para transacciones atómicas
- 🔐 **Autenticación**: Sistema de usuarios y permisos
- 📊 **Métricas**: Monitoreo de rendimiento en tiempo real
- 🌍 **Clustering**: Distribución en múltiples nodos
- 🤖 **Machine Learning**: Índices adaptativos inteligentes

### **Integraciones Posibles**
- 🐳 **Docker**: Containerización del sistema
- ☁️ **Cloud**: Despliegue en AWS/Azure/GCP
- 📱 **Mobile**: SDK para aplicaciones móviles
- 🌐 **WebSocket**: Actualizaciones en tiempo real

---

## 🤝 Contribuciones

### **Cómo Contribuir**
1. Fork el repositorio
2. Crear rama feature: `git checkout -b feature/nueva-funcionalidad`
3. Commit cambios: `git commit -am 'Agregar nueva funcionalidad'`
4. Push rama: `git push origin feature/nueva-funcionalidad`
5. Crear Pull Request

### **Estándares de Código**
- **Python**: PEP 8
- **Documentación**: Docstrings obligatorios
- **Testing**: Cobertura mínima 80%
- **Commits**: Conventional Commits

---

## 📄 Información del Proyecto

### **Autores**
- Desarrollado para el curso de **Base de Datos II**
- Universidad de Ingeniería y Tecnología (UTEC)
- Proyecto académico 2024-2025

### **Licencia**
Este proyecto es parte de un trabajo académico y está destinado únicamente para fines educativos.

### **Versión**
- **Actual**: v1.0.0
- **Python**: 3.8+
- **FastAPI**: 0.104+
- **Última actualización**: Mayo 2025

### **Soporte**
Para preguntas o issues:
1. Revisar esta documentación
2. Ejecutar tests de diagnóstico
3. Consultar logs del sistema
4. Crear issue en el repositorio

---

## 🎉 ¡Felicitaciones!

Has completado la instalación y configuración del **Sistema de Base de Datos Multimodal**. 

### **Próximos Pasos Recomendados:**
1. ✅ Ejecutar `python test_api.py` para validar instalación
2. ✅ Probar modo interactivo con `python main.py interactive`
3. ✅ Explorar API en `http://localhost:8000/docs`
4. ✅ Cargar tus propios datos CSV
5. ✅ Experimentar con diferentes tipos de índices

### **¡El sistema está listo para usar! 🚀**

