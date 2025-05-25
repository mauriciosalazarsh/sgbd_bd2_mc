# api.py (ACTUALIZADO para manejar CSV correctamente)
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
import os
import csv
import tempfile
import re
from parser_sql.parser import SQLParser
from engine import Engine

app = FastAPI(title="Sistema de Base de Datos Multimodal", 
              description="API para gestión de datos con indexación avanzada",
              version="1.0.0")

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

# Modelos Pydantic (sin cambios)
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

class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None

# ========== UTILIDAD PARA PARSEAR CSV ==========
# Reemplaza la función parse_csv_records en tu api.py con esto:

def parse_csv_records(records: List[str], headers: List[str]) -> Dict[str, Any]:
    """
    Parsea registros en formato CSV y los convierte al formato esperado por el frontend
    VERSIÓN CORREGIDA
    """
    if not records:
        return {
            "columns": headers,
            "rows": [],
            "count": 0
        }
    
    parsed_rows = []
    for record in records:
        if not record.strip():
            continue
            
        # Verificar si el record ya es una lista/array
        if isinstance(record, list):
            # Ya es una lista, usarla directamente
            row = [str(cell).strip() for cell in record]
            parsed_rows.append(row)
            continue
            
        # Si es string, parsearlo como CSV
        try:
            import io
            csv_reader = csv.reader(io.StringIO(record.strip()))
            row = next(csv_reader)
            # Limpiar cada celda
            cleaned_row = [str(cell).strip().strip('"') for cell in row]
            parsed_rows.append(cleaned_row)
        except Exception as e:
            print(f"Error parseando record: {record[:100]}... Error: {e}")
            # Fallback: split simple por comas
            row = [str(cell).strip().strip('"') for cell in record.split(',')]
            parsed_rows.append(row)
    
    # Verificar que todos los rows tengan el mismo número de columnas
    if parsed_rows:
        max_columns = max(len(row) for row in parsed_rows)
        min_columns = min(len(row) for row in parsed_rows)
        
        if max_columns != min_columns:
            print(f"⚠️ Inconsistencia en columnas: min={min_columns}, max={max_columns}")
            # Normalizar todas las filas al mismo número de columnas
            for row in parsed_rows:
                while len(row) < max_columns:
                    row.append("")
        
        # Ajustar headers si es necesario
        actual_columns = max_columns if parsed_rows else 0
        if len(headers) != actual_columns:
            print(f"⚠️ Headers ({len(headers)}) != Columnas de datos ({actual_columns})")
            if len(headers) < actual_columns:
                # Agregar headers faltantes
                for i in range(len(headers), actual_columns):
                    headers.append(f"column_{i}")
            else:
                # Recortar headers sobrantes
                headers = headers[:actual_columns]
    
    return {
        "columns": headers,
        "rows": parsed_rows,
        "count": len(parsed_rows)
    }

# ========== ENDPOINTS (actualizados) ==========

@app.get("/")
async def root():
    return {"message": "Sistema de Base de Datos Multimodal API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return APIResponse(
        success=True,
        message="Sistema funcionando correctamente",
        data={"status": "healthy", "tables": list(engine.tables.keys())}
    )

@app.post("/tables/create", response_model=APIResponse)
async def create_table(request: CreateTableRequest):
    """
    Crear una tabla cargando datos desde un archivo CSV
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
        if table_name not in engine.tables:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")
        
        headers = engine.get_table_headers(table_name)
        csv_path = engine.get_table_file_path(table_name)
        
        return APIResponse(
            success=True,
            message=f"Headers de tabla '{table_name}' desde {os.path.basename(csv_path)}",
            data={
                "headers": headers,
                "count": len(headers),
                "table_name": table_name,
                "csv_path": csv_path
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}/scan", response_model=APIResponse)
async def scan_table(table_name: str):
    """
    Obtener todos los registros de una tabla CON HEADERS dinámicos y formato CSV correcto
    """
    try:
        if table_name not in engine.tables:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")
        
        # Obtener datos en formato CSV
        result = engine.scan(table_name)
        records = result.split('\n') if result else []
        
        # Obtener headers dinámicamente del CSV original
        headers = engine.get_table_headers(table_name)
        csv_path = engine.get_table_file_path(table_name)
        
        # Parsear los datos CSV correctamente
        parsed_data = parse_csv_records(records, headers.copy())
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {parsed_data['count']} registros con {len(parsed_data['columns'])} columnas",
            data={
                "columns": parsed_data["columns"],     # ← FORMATO CORRECTO para frontend
                "rows": parsed_data["rows"],           # ← FORMATO CORRECTO para frontend
                "total_records": parsed_data["count"],
                "table_name": table_name,
                "csv_path": csv_path
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/records/search", response_model=APIResponse)
async def search_records(request: SearchRequest):
    """
    Buscar registros por clave exacta
    """
    try:
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
    Buscar registros en un rango de claves
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
    """
    Buscar registros usando consultas espaciales (solo para R-Tree)
    """
    try:
        results = engine.range_search(request.table_name, request.point, request.param)
        headers = engine.get_table_headers(request.table_name)
        
        # Para búsquedas espaciales, agregar columna de distancia si no existe
        if headers and "distance" not in [h.lower() for h in headers]:
            headers_with_distance = headers + ["distance"]
        else:
            headers_with_distance = headers
            
        parsed_data = parse_csv_records(results, headers_with_distance.copy())
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {parsed_data['count']} registros espaciales",
            data={
                "columns": parsed_data["columns"],
                "rows": parsed_data["rows"],
                "count": parsed_data["count"]
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/records/delete", response_model=APIResponse)
async def delete_records(request: DeleteRequest):
    """
    Eliminar registros por clave
    """
    try:
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
    Ejecutar consulta SQL con headers dinámicos y formato CSV correcto
    """
    try:
        result = sql_parser.parse_and_execute(request.query)
        
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
        if table_name and table_name in engine.tables:
            headers = engine.get_table_headers(table_name)
            csv_path = engine.get_table_file_path(table_name)
        
        # Procesar resultado
        if isinstance(result, str):
            if "registros" in result.lower() or "encontraron" in result.lower():
                # Es un mensaje de resultado
                parsed_data = {"columns": ["message"], "rows": [[result]], "count": 1}
            else:
                # Es un mensaje de operación exitosa
                parsed_data = {"columns": ["message"], "rows": [[result]], "count": 1}
        elif isinstance(result, list):
            # Es una lista de registros CSV
            parsed_data = parse_csv_records(result, headers.copy() if headers else ["column_1"])
        else:
            parsed_data = {"columns": ["result"], "rows": [[str(result)]], "count": 1}
            
        return APIResponse(
            success=True,
            message=f"Consulta ejecutada exitosamente",
            data={
                "columns": parsed_data["columns"],    # ← FORMATO CORRECTO
                "rows": parsed_data["rows"],          # ← FORMATO CORRECTO
                "count": parsed_data["count"], 
                "query": request.query,
                "table_name": table_name,
                "csv_path": csv_path
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables", response_model=APIResponse)
async def list_tables():
    """
    Listar todas las tablas disponibles con información de headers dinámicos
    """
    try:
        tables_info = {}
        for table_name, index in engine.tables.items():
            # Obtener información completa incluyendo headers
            table_info = engine.get_table_info(table_name)
            
            tables_info[table_name] = {
                "index_type": type(index).__name__,
                "field_index": getattr(index, 'field_index', None),
                "headers": table_info.get('headers', []),
                "headers_count": len(table_info.get('headers', [])),
                "csv_path": table_info.get('csv_path', '')
            }
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(tables_info)} tablas con headers dinámicos",
            data={"tables": tables_info, "count": len(tables_info)}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}/info", response_model=APIResponse)
async def get_table_info(table_name: str):
    """
    Obtener información completa de una tabla incluyendo headers dinámicos
    """
    try:
        if table_name not in engine.tables:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")
        
        # Obtener información completa de la tabla con headers
        table_info = engine.get_table_info(table_name)
        
        # Obtener muestra de datos en formato CSV correcto
        sample_records = []
        total_records = 0
        try:
            all_records_str = engine.scan(table_name)
            all_records = all_records_str.split('\n') if all_records_str else []
            
            headers = engine.get_table_headers(table_name)
            sample_data = parse_csv_records(all_records[:5], headers.copy())  # Primeros 5 registros
            
            sample_records = sample_data["rows"]
            total_records = len(all_records)
        except:
            pass
        
        # Combinar toda la información
        complete_info = {
            **table_info,  # Incluye headers, csv_path, etc.
            "sample_records": sample_records,
            "total_records": total_records
        }
        
        return APIResponse(
            success=True,
            message=f"Información completa de tabla '{table_name}' con {len(table_info.get('headers', []))} columnas",
            data=complete_info
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/headers/all", response_model=APIResponse)
async def get_all_table_headers():
    """
    Obtener headers de todas las tablas cargadas
    """
    try:
        all_headers = {}
        for table_name in engine.tables.keys():
            headers = engine.get_table_headers(table_name)
            csv_path = engine.get_table_file_path(table_name)
            all_headers[table_name] = {
                "headers": headers,
                "count": len(headers),
                "csv_path": csv_path,
                "csv_filename": os.path.basename(csv_path) if csv_path else ""
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