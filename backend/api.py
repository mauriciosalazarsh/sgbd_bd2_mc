from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
import os
import csv
import tempfile
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

# Modelos Pydantic
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

@app.get("/")
async def root():
    return {"message": "Sistema de Base de Datos Multimodal API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "tables": list(engine.tables.keys())}

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
        
        return APIResponse(
            success=True,
            message=result,
            data={"table_name": request.table_name, "index_type": request.index_type}
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

@app.get("/tables/{table_name}/scan", response_model=APIResponse)
async def scan_table(table_name: str):
    """
    Obtener todos los registros de una tabla
    """
    try:
        result = engine.scan(table_name)
        records = result.split('\n') if result else []
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(records)} registros",
            data={"records": records, "count": len(records)}
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
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(results)} registros",
            data={"records": results, "count": len(results)}
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
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(results)} registros en el rango",
            data={"records": results, "count": len(results)}
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
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(results)} registros espaciales",
            data={"records": results, "count": len(results)}
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
        
        return APIResponse(
            success=True,
            message=f"Se eliminaron {len(results)} registros",
            data={"deleted_records": results, "count": len(results)}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sql/execute", response_model=APIResponse)
async def execute_sql(request: SQLQueryRequest):
    """
    Ejecutar una consulta SQL personalizada
    """
    try:
        result = sql_parser.parse_and_execute(request.query)
        
        # Determinar el tipo de respuesta basado en el resultado
        if isinstance(result, str):
            if "registros" in result.lower() or "encontraron" in result.lower():
                # Es un mensaje de resultado
                records = []
                count = 0
            else:
                # Es un mensaje de operación exitosa
                records = [result]
                count = 1
        elif isinstance(result, list):
            records = result
            count = len(result)
        else:
            records = [str(result)]
            count = 1
            
        return APIResponse(
            success=True,
            message=f"Consulta ejecutada exitosamente",
            data={"records": records, "count": count, "query": request.query}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables", response_model=APIResponse)
async def list_tables():
    """
    Listar todas las tablas disponibles
    """
    try:
        tables_info = {}
        for table_name, index in engine.tables.items():
            tables_info[table_name] = {
                "index_type": type(index).__name__,
                "field_index": getattr(index, 'field_index', None)
            }
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(tables_info)} tablas",
            data={"tables": tables_info, "count": len(tables_info)}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}/info", response_model=APIResponse)
async def get_table_info(table_name: str):
    """
    Obtener información de una tabla específica
    """
    try:
        if table_name not in engine.tables:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")
        
        index = engine.tables[table_name]
        
        # Obtener muestra de datos
        sample_records = []
        try:
            all_records = engine.scan(table_name).split('\n')
            sample_records = all_records[:5]  # Primeros 5 registros
        except:
            pass
        
        table_info = {
            "name": table_name,
            "index_type": type(index).__name__,
            "field_index": getattr(index, 'field_index', None),
            "sample_records": sample_records,
            "total_records": len(all_records) if 'all_records' in locals() else 0
        }
        
        return APIResponse(
            success=True,
            message=f"Información de tabla '{table_name}'",
            data=table_info
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)