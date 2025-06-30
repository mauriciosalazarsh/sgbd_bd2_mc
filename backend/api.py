# api.py - INTEGRADO CON SISTEMA SPIMI
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
import os
import csv
import tempfile
import re
import time
from parser_sql.parser import SQLParser
from engine import Engine

app = FastAPI(title="Sistema de Base de Datos Multimodal", 
              description="API para gestión de datos con indexación avanzada y búsqueda textual",
              version="2.0.0")

# CORS middleware para permitir requests desde frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instancia global del engine y parser
engine = Engine()
sql_parser = SQLParser(engine)

# ========== MODELOS PYDANTIC (ORIGINALES + NUEVOS PARA SPIMI) ==========

class CreateTableRequest(BaseModel):
    table_name: str
    csv_file_path: str
    index_type: str
    index_field: int

class InsertRequest(BaseModel):
    table_name: str
    values: List[str]

class SearchRequest(BaseModel):
    table_name: str
    key: str
    column: int

class RangeSearchRequest(BaseModel):
    table_name: str
    begin_key: str
    end_key: str

class SpatialSearchRequest(BaseModel):
    table_name: str
    point: str  # "lat,lon"
    param: str  # radio (float) o k (int)

class DeleteRequest(BaseModel):
    table_name: str
    key: str

class SQLQueryRequest(BaseModel):
    query: str

# ========== NUEVOS MODELOS PARA SPIMI ==========

class CreateTextIndexRequest(BaseModel):
    table_name: str
    csv_file_path: str
    text_fields: List[str]  # Campos a indexar
    language: str = "spanish"  # Idioma por defecto

class TextSearchRequest(BaseModel):
    table_name: str
    query: str  # Consulta en lenguaje natural
    k: int = 10  # Top-K resultados
    fields: List[str] = ["*"]  # Campos a retornar

class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None

# ========== FUNCIÓN PARSE CSV (ACTUALIZADA PARA SPIMI) ==========

def parse_csv_records(records: List[Any], headers: List[str]) -> Dict[str, Any]:
    """
    Parsea registros en formato CSV, listas o tuplas 
    y los convierte al formato esperado por el frontend.
    ACTUALIZADA: Maneja resultados de búsquedas textuales con scores.
    """
    import csv
    import io
    
    parsed_rows = []
    
    for rec in records:
        row_data = []
        
        # Caso 1: Si es una lista directamente (ya procesada)
        if isinstance(rec, list):
            row_data = [str(cell).strip().strip('"') for cell in rec]
        
        # Caso 2: Si es una tupla del R-Tree (distancia, objeto)
        elif isinstance(rec, tuple) and len(rec) == 2:
            _, obj = rec  # obj es la fila original
            if isinstance(obj, list):
                row_data = [str(cell).strip().strip('"') for cell in obj]
            else:
                # Si obj es string, parsearlo como CSV
                try:
                    reader = csv.reader(io.StringIO(str(obj).strip()))
                    row_data = next(reader, [])
                except:
                    row_data = [str(obj)]
        
        # Caso 3: Si es un string CSV (formato común del B+Tree y SPIMI)
        elif isinstance(rec, str):
            try:
                # Manejo especial para strings con distancia
                if ',distance=' in rec:
                    # Separar la parte principal de la distancia
                    parts = rec.rsplit(',distance=', 1)
                    main_part = parts[0]
                    distance = parts[1]
                    
                    # Parsear la parte principal como CSV
                    reader = csv.reader(io.StringIO(main_part.strip()))
                    row_data = next(reader, [])
                    
                    # Añadir la distancia al final
                    row_data.append(distance)
                else:
                    # Parsear como CSV normal usando csv.reader para manejar comillas correctamente
                    reader = csv.reader(io.StringIO(rec.strip()))
                    row_data = next(reader, [])
                
            except Exception as e:
                print(f"Error parseando CSV: {e}, usando fallback")
                # Fallback: split por comas simple
                if ',distance=' in rec:
                    parts = rec.rsplit(',distance=', 1)
                    main_part = parts[0]
                    distance = parts[1]
                    row_data = [cell.strip().strip('"') for cell in main_part.split(',')]
                    row_data.append(distance)
                else:
                    row_data = [cell.strip().strip('"') for cell in rec.split(',')]
        
        # Caso 4: Cualquier otro tipo, convertir a string
        else:
            row_data = [str(rec)]
        
        # Limpiar datos de cada celda
        cleaned_row = []
        for cell in row_data:
            cleaned_cell = str(cell).strip()
            # Remover comillas extra si las hay
            if cleaned_cell.startswith('"') and cleaned_cell.endswith('"'):
                cleaned_cell = cleaned_cell[1:-1]
            cleaned_row.append(cleaned_cell)
        
        parsed_rows.append(cleaned_row)
    
    # Normalizar todas las filas al mismo número de columnas
    if parsed_rows:
        max_cols = max(len(r) for r in parsed_rows)
        
        # Completar filas cortas con strings vacíos
        for r in parsed_rows:
            while len(r) < max_cols:
                r.append("")
        
        # Ajustar headers al mismo tamaño
        adjusted_headers = headers.copy()
        while len(adjusted_headers) < max_cols:
            adjusted_headers.append(f"column_{len(adjusted_headers)}")
        
        # NUEVO: Detectar si la última columna es un score de similitud
        if parsed_rows and len(parsed_rows[0]) > 0:
            try:
                last_col_value = float(parsed_rows[0][-1])
                if 0 <= last_col_value <= 1:  # Probable score de similitud
                    if adjusted_headers[-1].startswith("column_"):
                        adjusted_headers[-1] = "similarity_score"
            except (ValueError, IndexError):
                pass
        
        # Si hay más headers que columnas, truncar
        adjusted_headers = adjusted_headers[:max_cols]
    else:
        adjusted_headers = headers

    print(f"📊 DEBUG parse_csv_records:")
    print(f"   - Records originales: {len(records)}")
    print(f"   - Parsed rows: {len(parsed_rows)}")
    print(f"   - Headers ajustados: {adjusted_headers}")
    print(f"   - Primera fila parseada: {parsed_rows[0] if parsed_rows else 'N/A'}")

    return {
        "columns": adjusted_headers,
        "rows": parsed_rows,
        "count": len(parsed_rows)
    }

# ========== ENDPOINTS ORIGINALES ==========

@app.get("/")
async def root():
    return {"message": "Sistema de Base de Datos Multimodal API con SPIMI", "version": "2.0.0"}

@app.get("/health")
async def health_check():
    # ACTUALIZADO: Incluir información de tablas textuales
    text_tables_count = len(getattr(engine, 'text_tables', {}))
    traditional_tables_count = len(engine.tables)
    
    return APIResponse(
        success=True,
        message="Sistema funcionando correctamente",
        data={
            "status": "healthy", 
            "traditional_tables": list(engine.tables.keys()),
            "text_tables": list(getattr(engine, 'text_tables', {}).keys()),
            "total_tables": traditional_tables_count + text_tables_count
        }
    )

@app.post("/tables/create", response_model=APIResponse)
async def create_table(request: CreateTableRequest):
    """
    Crear una tabla cargando datos desde un archivo CSV (índices tradicionales)
    """
    try:
        if not os.path.exists(request.csv_file_path):
            raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {request.csv_file_path}")
        
        # Validar tipo de índice
        valid_types = ['sequential', 'isam', 'hash', 'bplustree', 'rtree']
        if request.index_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Tipo de índice inválido. Válidos: {valid_types}")
        
        result = engine.load_csv(
            table=request.table_name,
            path=request.csv_file_path,
            tipo=request.index_type,
            index_field=request.index_field
        )
        
        # Obtener headers dinámicos de la tabla recién creada
        headers = engine.get_table_headers(request.table_name)
        
        return APIResponse(
            success=True,
            message=result,
            data={
                "table_name": request.table_name, 
                "index_type": request.index_type,
                "headers": headers,
                "headers_count": len(headers),
                "csv_path": request.csv_file_path
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== NUEVOS ENDPOINTS PARA SPIMI ==========

@app.post("/tables/create-text-index", response_model=APIResponse)
async def create_text_index(request: CreateTextIndexRequest):
    """
    Crear una tabla con índice textual SPIMI para búsqueda semántica
    """
    try:
        if not os.path.exists(request.csv_file_path):
            raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {request.csv_file_path}")
        
        # Verificar que el archivo tiene los campos especificados
        with open(request.csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            
            missing_fields = []
            for field in request.text_fields:
                if field not in headers:
                    missing_fields.append(field)
            
            if missing_fields:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Campos no encontrados en CSV: {missing_fields}. Disponibles: {headers}"
                )
        
        # Crear consulta SQL para SPIMI
        fields_str = ', '.join([f'"{field}"' for field in request.text_fields])
        sql_query = f'''CREATE TABLE {request.table_name} 
FROM FILE "{request.csv_file_path}" 
USING INDEX SPIMI ({fields_str});'''
        
        print(f"🔨 Ejecutando consulta SPIMI: {sql_query}")
        
        # Ejecutar a través del parser SQL
        start_time = time.time()
        result = sql_parser.parse_and_execute(sql_query)
        construction_time = time.time() - start_time
        
        # Obtener información de la tabla textual creada
        text_tables = getattr(engine, 'text_tables', {})
        table_info = text_tables.get(request.table_name, {})
        
        return APIResponse(
            success=True,
            message=result,
            data={
                "table_name": request.table_name,
                "index_type": "SPIMI",
                "text_fields": request.text_fields,
                "language": request.language,
                "construction_time": construction_time,
                "csv_path": request.csv_file_path,
                "index_path": table_info.get('index_path', ''),
                "headers": engine.get_table_headers(request.table_name)
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search/text", response_model=APIResponse)
async def text_search(request: TextSearchRequest):
    """
    Realizar búsqueda textual semántica usando TF-IDF y similitud de coseno
    """
    try:
        # Verificar que la tabla existe y es textual
        text_tables = getattr(engine, 'text_tables', {})
        if request.table_name not in text_tables:
            raise HTTPException(
                status_code=404, 
                detail=f"Tabla textual '{request.table_name}' no encontrada. Tablas textuales disponibles: {list(text_tables.keys())}"
            )
        
        # Construir consulta SQL con campos específicos
        if "*" in request.fields:
            fields_str = "*"
        else:
            fields_str = ", ".join(request.fields)
        
        # Usar el primer campo textual como campo de búsqueda por defecto
        text_info = text_tables[request.table_name]
        search_field = text_info['text_fields'][0] if text_info['text_fields'] else 'lyrics'
        
        sql_query = f'''SELECT {fields_str} FROM {request.table_name} 
WHERE {search_field} @@ '{request.query}' LIMIT {request.k};'''
        
        print(f"🔍 Ejecutando búsqueda textual: {sql_query}")
        
        # Ejecutar búsqueda
        start_time = time.time()
        results = sql_parser.parse_and_execute(sql_query)
        search_time = time.time() - start_time
        
        # Procesar resultados
        if isinstance(results, list):
            headers = engine.get_table_headers(request.table_name).copy()
            if "similarity_score" not in headers:
                headers.append("similarity_score")
            
            parsed_data = parse_csv_records(results, headers)
        else:
            parsed_data = {"columns": ["message"], "rows": [[str(results)]], "count": 0}
        
        return APIResponse(
            success=True,
            message=f"Búsqueda textual completada en {search_time:.4f}s",
            data={
                "query": request.query,
                "search_time": search_time,
                "table_name": request.table_name,
                "columns": parsed_data["columns"],
                "rows": parsed_data["rows"],
                "count": parsed_data["count"],
                "k": request.k
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/text-indexes", response_model=APIResponse)
async def list_text_indexes():
    """
    Listar todas las tablas con índices textuales SPIMI
    """
    try:
        text_tables = getattr(engine, 'text_tables', {})
        
        text_info = {}
        for table_name, info in text_tables.items():
            # Obtener estadísticas del índice si es posible
            try:
                # Intentar cargar estadísticas del archivo de índice
                import pickle
                index_path = info.get('index_path', '')
                stats = {}
                
                if os.path.exists(index_path):
                    with open(index_path, 'rb') as f:
                        index_data = pickle.load(f)
                        stats = {
                            'total_terms': len(index_data.get('index', {})),
                            'total_documents': index_data.get('total_documents', 0),
                            'language': index_data.get('language', 'spanish'),
                            'index_size_mb': round(os.path.getsize(index_path) / (1024 * 1024), 2)
                        }
            except:
                stats = {}
            
            text_info[table_name] = {
                "index_type": "SPIMI",
                "text_fields": info.get('text_fields', []),
                "csv_path": info.get('csv_path', ''),
                "index_path": info.get('index_path', ''),
                "headers": engine.get_table_headers(table_name),
                "stats": stats
            }
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(text_info)} índices textuales",
            data={"text_indexes": text_info, "count": len(text_info)}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}/text-info", response_model=APIResponse)
async def get_text_table_info(table_name: str):
    """
    Obtener información detallada de una tabla textual específica
    """
    try:
        text_tables = getattr(engine, 'text_tables', {})
        if table_name not in text_tables:
            raise HTTPException(
                status_code=404, 
                detail=f"Tabla textual '{table_name}' no encontrada"
            )
        
        info = text_tables[table_name]
        
        # Cargar estadísticas detalladas del índice
        stats = {}
        try:
            import pickle
            index_path = info.get('index_path', '')
            if os.path.exists(index_path):
                with open(index_path, 'rb') as f:
                    index_data = pickle.load(f)
                    
                    inverted_index = index_data.get('index', {})
                    
                    # Calcular estadísticas avanzadas
                    total_postings = sum(len(postings) for postings in inverted_index.values())
                    avg_postings = total_postings / len(inverted_index) if inverted_index else 0
                    
                    stats = {
                        'total_terms': len(inverted_index),
                        'total_documents': index_data.get('total_documents', 0),
                        'total_postings': total_postings,
                        'avg_postings_per_term': round(avg_postings, 2),
                        'language': index_data.get('language', 'spanish'),
                        'build_method': index_data.get('build_method', 'SPIMI'),
                        'index_size_mb': round(os.path.getsize(index_path) / (1024 * 1024), 2),
                        'has_precomputed_norms': 'document_norms' in index_data
                    }
        except Exception as e:
            print(f"Error cargando estadísticas: {e}")
        
        return APIResponse(
            success=True,
            message=f"Información de tabla textual '{table_name}'",
            data={
                "table_name": table_name,
                "index_type": "SPIMI",
                "text_fields": info.get('text_fields', []),
                "csv_path": info.get('csv_path', ''),
                "index_path": info.get('index_path', ''),
                "headers": engine.get_table_headers(table_name),
                "stats": stats
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== ENDPOINTS ORIGINALES (SIN CAMBIOS) ==========

@app.post("/tables/upload-csv")
async def upload_csv_file(file: UploadFile = File(...)):
    """
    Subir un archivo CSV al servidor
    """
    try:
        # Crear directorio de datos si no existe
        os.makedirs("datos", exist_ok=True)
        
        # Guardar archivo subido
        file_path = f"datos/{file.filename}"
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Leer preview del archivo
        preview = []
        with open(file_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            for i, row in enumerate(reader):
                if i >= 5:  # Solo 5 filas de preview
                    break
                preview.append(row)
        
        return APIResponse(
            success=True,
            message=f"Archivo {file.filename} subido exitosamente",
            data={
                "file_path": file_path,
                "headers": headers,
                "preview": preview,
                "columns_count": len(headers)
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/records/insert", response_model=APIResponse)
async def insert_record(request: InsertRequest):
    """
    Insertar un registro en una tabla
    """
    try:
        result = engine.insert(request.table_name, request.values)
        return APIResponse(
            success=True,
            message=result,
            data={"inserted_values": request.values}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}/headers", response_model=APIResponse)
async def get_table_headers(table_name: str):
    """
    Obtener headers/columnas de cualquier tabla (dinámico desde CSV original)
    """
    try:
        # Verificar tanto en tablas tradicionales como textuales
        text_tables = getattr(engine, 'text_tables', {})
        if table_name not in engine.tables and table_name not in text_tables:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")
        
        headers = engine.get_table_headers(table_name)
        csv_path = engine.get_table_file_path(table_name)
        
        # Determinar tipo de tabla
        table_type = "textual" if table_name in text_tables else "traditional"
        
        return APIResponse(
            success=True,
            message=f"Headers de tabla '{table_name}' ({table_type}) desde {os.path.basename(csv_path)}",
            data={
                "headers": headers,
                "count": len(headers),
                "table_name": table_name,
                "table_type": table_type,
                "csv_path": csv_path
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}/scan", response_model=APIResponse)
async def scan_table(table_name: str):
    try:
        # Verificar tanto tablas tradicionales como textuales
        text_tables = getattr(engine, 'text_tables', {})
        if table_name not in engine.tables and table_name not in text_tables:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")

        # NOTA: Las tablas textuales no soportan scan directo, solo búsquedas
        if table_name in text_tables:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{table_name}' es textual. Use /search/text para búsquedas semánticas."
            )

        result = engine.scan(table_name)
        headers = engine.get_table_headers(table_name)

        # Si result es lista de tuplas → R-Tree
        if isinstance(result, list) and result and isinstance(result[0], tuple) and len(result[0]) == 2:
            records = [row for _, row in result]  # Sacar solo el "objeto" o fila original
        elif isinstance(result, str):
            records = result.strip().split('\n')
        else:
            records = result  # fallback

        parsed_data = parse_csv_records(records, headers.copy())

        return APIResponse(
            success=True,
            message=f"Se encontraron {parsed_data['count']} registros",
            data={
                "columns": parsed_data["columns"],
                "rows": parsed_data["rows"],
                "total_records": parsed_data["count"],
                "table_name": table_name,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/records/search", response_model=APIResponse)
async def search_records(request: SearchRequest):
    """
    Buscar registros por clave exacta (solo tablas tradicionales)
    """
    try:
        # Verificar que no sea tabla textual
        text_tables = getattr(engine, 'text_tables', {})
        if request.table_name in text_tables:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{request.table_name}' es textual. Use /search/text para búsquedas semánticas."
            )

        results = engine.search(request.table_name, request.key, request.column)
        headers = engine.get_table_headers(request.table_name)
        
        # Parsear resultados CSV
        parsed_data = parse_csv_records(results, headers.copy())
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {parsed_data['count']} registros",
            data={
                "columns": parsed_data["columns"],
                "rows": parsed_data["rows"], 
                "count": parsed_data["count"]
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/records/range-search", response_model=APIResponse)
async def range_search_records(request: RangeSearchRequest):
    """
    Buscar registros en un rango de claves (solo tablas tradicionales)
    """
    try:
        results = engine.range_search(request.table_name, request.begin_key, request.end_key)
        headers = engine.get_table_headers(request.table_name)
        
        # Parsear resultados CSV
        parsed_data = parse_csv_records(results, headers.copy())
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {parsed_data['count']} registros en el rango",
            data={
                "columns": parsed_data["columns"],
                "rows": parsed_data["rows"],
                "count": parsed_data["count"]
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/records/spatial-search", response_model=APIResponse)
async def spatial_search_records(request: SpatialSearchRequest):
    try:
        # El engine ya maneja todo el procesamiento interno del R-Tree
        raw = engine.range_search(request.table_name, request.point, request.param)
        headers = engine.get_table_headers(request.table_name)

        # Agregar distance al header si no existe (para R-Tree)
        if "distance" not in [h.lower() for h in headers]:
            headers = headers + ["distance"]

        # raw ya viene como lista de strings CSV procesadas por el engine
        # NO necesitamos desempaquetar tuplas aquí
        parsed = parse_csv_records(raw, headers.copy())

        return APIResponse(
            success=True,
            message=f"Se encontraron {parsed['count']} registros espaciales",
            data={
                "columns": parsed["columns"],
                "rows": parsed["rows"],
                "count": parsed["count"]
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/records/delete", response_model=APIResponse)
async def delete_records(request: DeleteRequest):
    """
    Eliminar registros por clave (solo tablas tradicionales)
    """
    try:
        # Verificar que no sea tabla textual
        text_tables = getattr(engine, 'text_tables', {})
        if request.table_name in text_tables:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{request.table_name}' es textual. Eliminación no soportada para índices SPIMI."
            )

        results = engine.remove(request.table_name, request.key)
        headers = engine.get_table_headers(request.table_name)
        
        # Parsear resultados CSV
        parsed_data = parse_csv_records(results, headers.copy())
        
        return APIResponse(
            success=True,
            message=f"Se eliminaron {parsed_data['count']} registros",
            data={
                "columns": parsed_data["columns"],
                "rows": parsed_data["rows"],
                "count": parsed_data["count"]
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sql/execute", response_model=APIResponse)
async def execute_sql(request: SQLQueryRequest):
    """
    Ejecutar consulta SQL (tradicionales + SPIMI con @@)
    """
    try:
        start_time = time.time()
        result = sql_parser.parse_and_execute(request.query)
        execution_time = time.time() - start_time
        
        # Detectar tabla de la consulta
        table_name = None
        query_lower = request.query.lower()
        if 'from ' in query_lower:
            # Extraer nombre de tabla usando regex
            match = re.search(r'from\s+(\w+)', query_lower)
            if match:
                table_name = match.group(1)
        
        # Obtener headers dinámicamente si es una consulta SELECT
        headers = []
        csv_path = ""
        if table_name:
            # Buscar en tablas tradicionales y textuales
            if table_name in engine.tables or table_name in getattr(engine, 'text_tables', {}):
                headers = engine.get_table_headers(table_name)
                csv_path = engine.get_table_file_path(table_name)
        
        # Procesar resultado
        if isinstance(result, str):
            if any(word in result.lower() for word in ["registros", "encontraron", "tabla", "creada", "índice"]):
                # Es un mensaje de resultado
                parsed_data = {"columns": ["message"], "rows": [[result]], "count": 1}
            else:
                parsed_data = {"columns": ["message"], "rows": [[result]], "count": 1}
        elif isinstance(result, list):
            # Para búsquedas textuales, agregar similarity_score a headers si no existe
            if ' @@ ' in request.query and headers:
                headers_copy = headers.copy()
                if "similarity_score" not in headers_copy:
                    headers_copy.append("similarity_score")
            else:
                headers_copy = headers.copy() if headers else ["column_1"]
            
            parsed_data = parse_csv_records(result, headers_copy)
        else:
            parsed_data = {"columns": ["result"], "rows": [[str(result)]], "count": 1}
        
        # Detectar tipo de consulta para mensaje apropiado
        query_type = "unknown"
        if ' @@ ' in request.query:
            query_type = "text_search"
        elif query_lower.startswith('create table') and 'spimi' in query_lower:
            query_type = "create_text_index"
        elif query_lower.startswith('select'):
            query_type = "select"
        elif query_lower.startswith('create'):
            query_type = "create_table"
            
        return APIResponse(
            success=True,
            message=f"Consulta {query_type} ejecutada exitosamente en {execution_time:.4f}s",
            data={
                "columns": parsed_data["columns"],    
                "rows": parsed_data["rows"],          
                "count": parsed_data["count"], 
                "query": request.query,
                "query_type": query_type,
                "execution_time": execution_time,
                "table_name": table_name,
                "csv_path": csv_path
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables", response_model=APIResponse)
async def list_tables():
    """
    Listar todas las tablas disponibles (tradicionales + textuales)
    """
    try:
        all_tables_info = {}
        
        # Tablas tradicionales
        for table_name, index in engine.tables.items():
            table_info = engine.get_table_info(table_name)
            all_tables_info[table_name] = {
                "type": "traditional",
                "index_type": type(index).__name__,
                "field_index": getattr(index, 'field_index', None),
                "headers": table_info.get('headers', []),
                "headers_count": len(table_info.get('headers', [])),
                "csv_path": table_info.get('csv_path', '')
            }
        
        # Tablas textuales
        text_tables = getattr(engine, 'text_tables', {})
        for table_name, info in text_tables.items():
            all_tables_info[table_name] = {
                "type": "textual",
                "index_type": "SPIMI",
                "text_fields": info.get('text_fields', []),
                "headers": engine.get_table_headers(table_name),
                "headers_count": len(engine.get_table_headers(table_name)),
                "csv_path": info.get('csv_path', ''),
                "index_path": info.get('index_path', '')
            }
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(all_tables_info)} tablas ({len(engine.tables)} tradicionales, {len(text_tables)} textuales)",
            data={
                "tables": all_tables_info, 
                "count": len(all_tables_info),
                "traditional_count": len(engine.tables),
                "textual_count": len(text_tables)
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}/info", response_model=APIResponse)
async def get_table_info(table_name: str):
    """
    Obtener información completa de una tabla (tradicional o textual)
    """
    try:
        text_tables = getattr(engine, 'text_tables', {})
        
        if table_name not in engine.tables and table_name not in text_tables:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")
        
        # Determinar tipo de tabla y obtener información
        if table_name in text_tables:
            # Tabla textual
            text_info = text_tables[table_name]
            complete_info = {
                "table_name": table_name,
                "type": "textual",
                "index_type": "SPIMI",
                "text_fields": text_info.get('text_fields', []),
                "headers": engine.get_table_headers(table_name),
                "headers_count": len(engine.get_table_headers(table_name)),
                "csv_path": text_info.get('csv_path', ''),
                "index_path": text_info.get('index_path', ''),
                "sample_records": [],  # Las tablas textuales no soportan scan
                "total_records": 0
            }
        else:
            # Tabla tradicional
            table_info = engine.get_table_info(table_name)
            
            # Obtener muestra de datos
            sample_records = []
            total_records = 0
            try:
                all_records_str = engine.scan(table_name)
                all_records = all_records_str.split('\n') if all_records_str else []
                
                headers = engine.get_table_headers(table_name)
                sample_data = parse_csv_records(all_records[:5], headers.copy())
                
                sample_records = sample_data["rows"]
                total_records = len(all_records)
            except:
                pass
            
            complete_info = {
                **table_info,
                "type": "traditional",
                "sample_records": sample_records,
                "total_records": total_records
            }
        
        return APIResponse(
            success=True,
            message=f"Información completa de tabla '{table_name}' ({complete_info['type']})",
            data=complete_info
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/headers/all", response_model=APIResponse)
async def get_all_table_headers():
    """
    Obtener headers de todas las tablas cargadas (tradicionales + textuales)
    """
    try:
        all_headers = {}
        
        # Headers de tablas tradicionales
        for table_name in engine.tables.keys():
            headers = engine.get_table_headers(table_name)
            csv_path = engine.get_table_file_path(table_name)
            all_headers[table_name] = {
                "type": "traditional",
                "headers": headers,
                "count": len(headers),
                "csv_path": csv_path,
                "csv_filename": os.path.basename(csv_path) if csv_path else ""
            }
        
        # Headers de tablas textuales
        text_tables = getattr(engine, 'text_tables', {})
        for table_name in text_tables.keys():
            headers = engine.get_table_headers(table_name)
            csv_path = engine.get_table_file_path(table_name)
            all_headers[table_name] = {
                "type": "textual",
                "headers": headers,
                "count": len(headers),
                "csv_path": csv_path,
                "csv_filename": os.path.basename(csv_path) if csv_path else "",
                "text_fields": text_tables[table_name].get('text_fields', [])
            }
        
        return APIResponse(
            success=True,
            message=f"Headers de {len(all_headers)} tablas",
            data={"tables_headers": all_headers}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)