# api.py - VERSIÓN COMPLETA CON MULTIMEDIA
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
import os
import csv
import tempfile
import re
import time
import shutil
import pickle
import numpy as np  # Para cargar pickles con arrays numpy
from parser_sql.parser import SQLParser
from engine import Engine

app = FastAPI(title="Sistema de Base de Datos Multimodal", 
              description="API completa para gestión de datos con indexación tradicional, textual y multimedia",
              version="3.0.0")

# CORS middleware para permitir requests desde frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static directories for serving media files
if os.path.exists("datos"):
    app.mount("/datos", StaticFiles(directory="datos"), name="datos")

# Instancia global del engine y parser
engine = Engine()
sql_parser = SQLParser(engine)

# ========== MODELOS PYDANTIC EXISTENTES ==========

class CreateTableRequest(BaseModel):
    table_name: str
    csv_file_path: str
    index_type: str
    index_field: int

class LoadPickleTableRequest(BaseModel):
    table_name: str
    pickle_file_path: str
    index_type: Optional[str] = "sequential"  # Tipo de índice por defecto

class LoadMultimediaFromPicklesRequest(BaseModel):
    table_name: str
    histograms_path: str
    codebook_path: str
    features_path: str
    media_type: str  # 'image' o 'audio'
    csv_path: Optional[str] = None  # Path al CSV original si está disponible

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

class CreateTextIndexRequest(BaseModel):
    table_name: str
    csv_file_path: str
    text_fields: List[str]
    language: str = "spanish"

class TextSearchRequest(BaseModel):
    table_name: str
    query: str
    k: int = 10
    fields: List[str] = ["*"]

# ========== NUEVOS MODELOS PARA MULTIMEDIA ==========

class CreateMultimediaTableRequest(BaseModel):
    table_name: str
    csv_file_path: str
    media_type: str  # 'image' o 'audio'
    feature_method: str  # 'sift', 'resnet50', 'inception_v3', 'mfcc', 'spectrogram', 'comprehensive'
    n_clusters: int = 256
    path_column: Optional[str] = None  # Columna con rutas de archivos
    base_path: Optional[str] = ""  # Ruta base para archivos

class MultimediaSearchRequest(BaseModel):
    table_name: str
    query_file_path: str  # Ruta del archivo de consulta
    k: int = 10
    method: str = "inverted"  # 'sequential' o 'inverted'
    fields: List[str] = ["*"]

class MultimediaBenchmarkRequest(BaseModel):
    table_name: str
    query_file_path: str
    k: int = 10

class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None

# ========== FUNCIÓN PARSE CSV (ACTUALIZADA PARA MULTIMEDIA) ==========

def parse_csv_records(records: List[Any], headers: List[str]) -> Dict[str, Any]:
    """
    Parsea registros en formato CSV, listas o tuplas 
    y los convierte al formato esperado por el frontend.
    CORREGIDA: Maneja correctamente el alineamiento de columnas.
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
                # Manejo especial para strings con distancia o score
                if ',similarity=' in rec or ',distance=' in rec:
                    # Separar la parte principal del score
                    if ',similarity=' in rec:
                        parts = rec.rsplit(',similarity=', 1)
                        score_label = 'similarity'
                    else:
                        parts = rec.rsplit(',distance=', 1)
                        score_label = 'distance'
                    
                    main_part = parts[0]
                    score = parts[1]
                    
                    # Parsear la parte principal como CSV
                    reader = csv.reader(io.StringIO(main_part.strip()))
                    row_data = next(reader, [])
                    
                    # Añadir el score al final
                    row_data.append(score)
                else:
                    # Parsear como CSV normal usando csv.reader para manejar comillas correctamente
                    reader = csv.reader(io.StringIO(rec.strip()))
                    row_data = next(reader, [])
                
            except Exception as e:
                print(f"Error parseando CSV: {e}, usando fallback")
                # Fallback: split por comas simple
                if ',similarity=' in rec or ',distance=' in rec:
                    if ',similarity=' in rec:
                        parts = rec.rsplit(',similarity=', 1)
                    else:
                        parts = rec.rsplit(',distance=', 1)
                    
                    main_part = parts[0]
                    score = parts[1]
                    row_data = [cell.strip().strip('"') for cell in main_part.split(',')]
                    row_data.append(score)
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
            # CORREGIDO: Detectar si la última columna es un score de similitud
            if len(adjusted_headers) == max_cols - 1:
                # Verificar si parece un score de similitud
                try:
                    sample_val = float(parsed_rows[0][-1]) if parsed_rows[0] else 0
                    if 0 <= sample_val <= 1:
                        adjusted_headers.append("similarity_score")
                    else:
                        adjusted_headers.append(f"column_{len(adjusted_headers)}")
                except (ValueError, IndexError):
                    adjusted_headers.append(f"column_{len(adjusted_headers)}")
            else:
                adjusted_headers.append(f"column_{len(adjusted_headers)}")
        
        # Si hay más headers que columnas, truncar
        adjusted_headers = adjusted_headers[:max_cols]
    else:
        adjusted_headers = headers

    print(f"DEBUG parse_csv_records:")
    print(f"   - Records originales: {len(records)}")
    print(f"   - Parsed rows: {len(parsed_rows)}")
    print(f"   - Headers originales: {headers}")
    print(f"   - Headers ajustados: {adjusted_headers}")
    print(f"   - Primera fila parseada: {parsed_rows[0] if parsed_rows else 'N/A'}")

    return {
        "columns": adjusted_headers,  # CORREGIDO: Usar "columns" no "headers"
        "rows": parsed_rows,
        "count": len(parsed_rows)
    }

# ========== ENDPOINTS BÁSICOS ==========

@app.get("/")
async def root():
    return {"message": "Sistema de Base de Datos Multimodal API Completa", "version": "3.0.0"}

@app.get("/health")
async def health_check():
    # Incluir información de todas las tablas
    multimedia_tables_count = len(sql_parser.multimedia_engines)
    text_tables_count = len(getattr(engine, 'text_tables', {}))
    embedding_tables_count = len(getattr(engine, 'embedding_tables', {}))
    traditional_tables_count = len(engine.tables) - multimedia_tables_count  # Restar multimedia que están en engine.tables
    
    return APIResponse(
        success=True,
        message="Sistema funcionando correctamente",
        data={
            "status": "healthy", 
            "traditional_tables": [t for t in engine.tables.keys() if t not in sql_parser.multimedia_engines],
            "text_tables": list(getattr(engine, 'text_tables', {}).keys()),
            "multimedia_tables": list(sql_parser.multimedia_engines.keys()),
            "embedding_tables": list(getattr(engine, 'embedding_tables', {}).keys()),
            "total_tables": traditional_tables_count + text_tables_count + multimedia_tables_count + embedding_tables_count
        }
    )

# ========== ENDPOINTS MULTIMEDIA ==========

@app.post("/multimedia/load-from-pickles", response_model=APIResponse)
async def load_multimedia_from_pickles(request: LoadMultimediaFromPicklesRequest):
    """
    Reconstruir una tabla multimedia desde archivos pickle existentes
    """
    try:
        # Validar que los archivos existen
        for path, name in [(request.histograms_path, "histogramas"), 
                          (request.codebook_path, "codebook"), 
                          (request.features_path, "features")]:
            if not os.path.exists(path):
                raise HTTPException(status_code=404, detail=f"Archivo de {name} no encontrado: {path}")
        
        # Validar tipo de media
        if request.media_type not in ['image', 'audio']:
            raise HTTPException(status_code=400, detail=f"Tipo de media inválido: {request.media_type}")
        
        # Importar módulos necesarios
        from multimedia.multimedia_engine import MultimediaEngine
        
        # Determinar método de características basado en el nombre del archivo
        feature_method = 'sift'  # Por defecto
        if 'sift' in request.codebook_path.lower():
            feature_method = 'sift'
        elif 'resnet' in request.codebook_path.lower():
            feature_method = 'resnet50'
        elif 'inception' in request.codebook_path.lower():
            feature_method = 'inception_v3'
        elif 'mfcc' in request.codebook_path.lower():
            feature_method = 'mfcc'
        elif 'spectrogram' in request.codebook_path.lower():
            feature_method = 'spectrogram'
        
        print(f"Reconstruyendo tabla multimedia '{request.table_name}' desde pickles")
        print(f"  Media type: {request.media_type}")
        print(f"  Feature method detectado: {feature_method}")
        
        # Crear motor multimedia
        multimedia_engine = MultimediaEngine(
            media_type=request.media_type,
            feature_method=feature_method
        )
        
        # Cargar datos desde los pickles
        # Cargar features
        print("Cargando features...")
        multimedia_engine.features_data = multimedia_engine.feature_extractor.load_features(request.features_path)
        print(f"Features cargadas: {len(multimedia_engine.features_data)} archivos")
        
        # Cargar codebook
        print("Cargando codebook...")
        multimedia_engine.codebook_builder.load_codebook(request.codebook_path)
        
        # Cargar histogramas
        print("Cargando histogramas...")
        with open(request.histograms_path, 'rb') as f:
            multimedia_engine.histograms_data = pickle.load(f)
        print(f"Histogramas cargados: {len(multimedia_engine.histograms_data)} vectores")
        
        # Configurar índices de búsqueda
        print("Configurando índices de búsqueda...")
        multimedia_engine.knn_inverted.build_index(multimedia_engine.histograms_data)
        multimedia_engine.knn_sequential.build_database(multimedia_engine.histograms_data)
        multimedia_engine.is_built = True
        print("Motor multimedia configurado correctamente")
        
        # Si se proporciona CSV, cargar metadata
        if request.csv_path and os.path.exists(request.csv_path):
            import pandas as pd
            df = pd.read_csv(request.csv_path)
            multimedia_engine.metadata_df = df
            print(f"  Metadata cargada desde CSV: {len(df)} registros")
        
        # Registrar en el parser SQL
        sql_parser.multimedia_engines[request.table_name] = multimedia_engine
        
        # También registrar en el engine principal para que aparezca en las tablas
        if hasattr(multimedia_engine, 'metadata_df') and multimedia_engine.metadata_df is not None:
            headers = list(multimedia_engine.metadata_df.columns)
        else:
            headers = ['filename', 'similarity_score']
        
        # Usar la instancia global del engine principal
        engine.table_headers[request.table_name] = headers
        
        print(f"Tabla multimedia '{request.table_name}' registrada en sql_parser.multimedia_engines")
        print(f"Tablas multimedia disponibles: {list(sql_parser.multimedia_engines.keys())}")
        
        return APIResponse(
            success=True,
            message=f"Tabla multimedia '{request.table_name}' reconstruida exitosamente desde pickles",
            data={
                "table_name": request.table_name,
                "media_type": request.media_type,
                "feature_method": feature_method,
                "features_loaded": len(multimedia_engine.features_data) if hasattr(multimedia_engine, 'features_data') else 0,
                "histograms_loaded": len(multimedia_engine.histograms_data) if hasattr(multimedia_engine, 'histograms_data') else 0,
                "is_built": multimedia_engine.is_built
            }
        )
    
    except ImportError as e:
        raise HTTPException(status_code=500, detail=f"Error importando módulos multimedia: {str(e)}")
    except Exception as e:
        print(f"Error reconstruyendo tabla multimedia: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/multimedia/create-table", response_model=APIResponse)
async def create_multimedia_table(request: CreateMultimediaTableRequest):
    """
    Crear una tabla multimedia con extracción de características y búsqueda por similitud
    """
    try:
        if not os.path.exists(request.csv_file_path):
            raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {request.csv_file_path}")
        
        # Validar parámetros
        valid_media_types = ['image', 'audio']
        if request.media_type not in valid_media_types:
            raise HTTPException(status_code=400, detail=f"Tipo de media inválido. Válidos: {valid_media_types}")
        
        valid_image_methods = ['sift', 'resnet50', 'inception_v3']
        valid_audio_methods = ['mfcc', 'spectrogram', 'comprehensive']
        
        if request.media_type == 'image' and request.feature_method not in valid_image_methods:
            raise HTTPException(status_code=400, detail=f"Método para imágenes inválido. Válidos: {valid_image_methods}")
        elif request.media_type == 'audio' and request.feature_method not in valid_audio_methods:
            raise HTTPException(status_code=400, detail=f"Método para audio inválido. Válidos: {valid_audio_methods}")
        
        # Construir consulta SQL multimedia
        sql_query = f'''CREATE MULTIMEDIA TABLE {request.table_name} 
FROM FILE "{request.csv_file_path}" 
USING {request.media_type} WITH METHOD {request.feature_method} CLUSTERS {request.n_clusters};'''
        
        print(f"Ejecutando consulta multimedia: {sql_query}")
        
        # Ejecutar a través del parser SQL
        start_time = time.time()
        result = sql_parser.parse_and_execute(sql_query)
        construction_time = time.time() - start_time
        
        # Obtener información del motor multimedia creado
        multimedia_info = sql_parser.get_multimedia_table_info(request.table_name)
        
        return APIResponse(
            success=True,
            message=result,
            data={
                "table_name": request.table_name,
                "media_type": request.media_type,
                "feature_method": request.feature_method,
                "n_clusters": request.n_clusters,
                "construction_time": construction_time,
                "csv_path": request.csv_file_path,
                "multimedia_info": multimedia_info
            }
        )
    
    except Exception as e:
        print(f"Error creando tabla multimedia: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/multimedia/search", response_model=APIResponse)
async def multimedia_search(request: MultimediaSearchRequest):
    """
    Realizar búsqueda multimedia por similitud usando el operador <->
    """
    try:
        # Verificar que el archivo de consulta existe
        if not os.path.exists(request.query_file_path):
            raise HTTPException(status_code=404, detail=f"Archivo de consulta no encontrado: {request.query_file_path}")
        
        # Verificar que la tabla multimedia existe
        if request.table_name not in sql_parser.multimedia_engines:
            multimedia_tables = list(sql_parser.multimedia_engines.keys())
            raise HTTPException(
                status_code=404, 
                detail=f"Tabla multimedia '{request.table_name}' no encontrada. Tablas disponibles: {multimedia_tables}"
            )
        
        # Construir campos para SELECT
        if "*" in request.fields:
            fields_str = "*"
        else:
            fields_str = ", ".join(request.fields)
        
        # Construir consulta SQL multimedia
        sql_query = f'''SELECT {fields_str} FROM {request.table_name} 
WHERE similarity <-> "{request.query_file_path}" METHOD {request.method} LIMIT {request.k};'''
        
        print(f"Ejecutando búsqueda multimedia: {sql_query}")
        
        # Ejecutar búsqueda
        start_time = time.time()
        results = sql_parser.parse_and_execute(sql_query)
        search_time = time.time() - start_time
        
        # Procesar resultados según el tipo retornado
        if isinstance(results, dict) and 'results' in results:
            # Resultado estructurado de multimedia
            multimedia_results = results['results']
            
            # Convertir a formato CSV para el frontend
            if multimedia_results:
                headers = engine.get_table_headers(request.table_name).copy()
                if "similarity_score" not in headers:
                    headers.extend(["filename", "similarity_score"])
                
                csv_rows = []
                for result in multimedia_results:
                    filename = result.get('filename', '')
                    similarity = result.get('similarity', 0.0)
                    metadata = result.get('metadata', {})
                    
                    # Crear fila CSV
                    csv_row = []
                    for header in headers:
                        if header == 'filename':
                            csv_row.append(filename)
                        elif header == 'similarity_score':
                            csv_row.append(str(similarity))
                        else:
                            value = metadata.get(header, '')
                            csv_row.append(str(value))
                    
                    csv_rows.append(csv_row)
                
                parsed_data = {
                    "columns": headers,
                    "rows": csv_rows,
                    "count": len(csv_rows)
                }
            else:
                parsed_data = {"columns": ["message"], "rows": [["No se encontraron resultados"]], "count": 0}
            
            return APIResponse(
                success=True,
                message=f"Búsqueda multimedia completada en {results.get('execution_time', search_time):.4f}s",
                data={
                    "query_file": request.query_file_path,
                    "search_time": results.get('execution_time', search_time),
                    "table_name": request.table_name,
                    "method": request.method,
                    "columns": parsed_data["columns"],
                    "rows": parsed_data["rows"],
                    "count": parsed_data["count"],
                    "k": request.k,
                    "multimedia_stats": results.get('stats', {})
                }
            )
        else:
            # Resultado no reconocido
            return APIResponse(
                success=False,
                message="Formato de resultados multimedia no reconocido",
                data={"raw_result": str(results)}
            )
    
    except Exception as e:
        print(f"Error en búsqueda multimedia: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/multimedia/tables", response_model=APIResponse)
async def list_multimedia_tables():
    """
    Listar todas las tablas multimedia disponibles
    """
    try:
        multimedia_tables = sql_parser.list_multimedia_tables()
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(multimedia_tables)} tablas multimedia",
            data={"multimedia_tables": multimedia_tables, "count": len(multimedia_tables)}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/multimedia/tables/{table_name}/info", response_model=APIResponse)
async def get_multimedia_table_info(table_name: str):
    """
    Obtener información detallada de una tabla multimedia específica
    """
    try:
        if table_name not in sql_parser.multimedia_engines:
            raise HTTPException(
                status_code=404, 
                detail=f"Tabla multimedia '{table_name}' no encontrada"
            )
        
        multimedia_info = sql_parser.get_multimedia_table_info(table_name)
        
        # Obtener estadísticas del motor multimedia
        engine_stats = sql_parser.multimedia_engines[table_name].get_system_statistics()
        
        return APIResponse(
            success=True,
            message=f"Información de tabla multimedia '{table_name}'",
            data={
                **multimedia_info,
                "system_stats": engine_stats
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/multimedia/benchmark", response_model=APIResponse)
async def multimedia_benchmark(request: MultimediaBenchmarkRequest):
    """
    Realizar benchmark de rendimiento entre métodos de búsqueda multimedia
    """
    try:
        # Verificar que el archivo de consulta existe
        if not os.path.exists(request.query_file_path):
            raise HTTPException(status_code=404, detail=f"Archivo de consulta no encontrado: {request.query_file_path}")
        
        # Verificar que la tabla multimedia existe
        if request.table_name not in sql_parser.multimedia_engines:
            raise HTTPException(
                status_code=404, 
                detail=f"Tabla multimedia '{request.table_name}' no encontrada"
            )
        
        multimedia_engine = sql_parser.multimedia_engines[request.table_name]
        
        # Ejecutar benchmark
        print(f"Ejecutando benchmark multimedia para tabla '{request.table_name}'")
        benchmark_results = multimedia_engine.benchmark_search_methods(
            request.query_file_path, 
            k=request.k
        )
        
        return APIResponse(
            success=True,
            message=f"Benchmark completado para tabla '{request.table_name}'",
            data={
                "table_name": request.table_name,
                "query_file": request.query_file_path,
                "k": request.k,
                "benchmark_results": benchmark_results
            }
        )
    
    except Exception as e:
        print(f"Error en benchmark multimedia: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/multimedia/upload-file")
async def upload_multimedia_file(file: UploadFile = File(...)):
    """
    Subir un archivo multimedia para usar en consultas
    """
    try:
        # Validar tipo de archivo
        allowed_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.wav', '.mp3', '.flac', '.ogg'}
        if not file.filename:
            raise HTTPException(status_code=400, detail="Filename is missing.")
        file_extension = os.path.splitext(file.filename)[1].lower()
        
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400, 
                detail=f"Tipo de archivo no soportado. Permitidos: {allowed_extensions}"
            )
        
        # Crear directorio de uploads si no existe
        upload_dir = "uploads/multimedia"
        os.makedirs(upload_dir, exist_ok=True)
        
        # Guardar archivo
        if not file.filename:
            raise HTTPException(status_code=400, detail="Filename is missing.")
        file_path = os.path.join(upload_dir, file.filename)
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Determinar tipo de media
        image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif'}
        audio_extensions = {'.wav', '.mp3', '.flac', '.ogg'}
        
        if file_extension in image_extensions:
            media_type = "image"
        elif file_extension in audio_extensions:
            media_type = "audio"
        else:
            media_type = "unknown"
        
        return APIResponse(
            success=True,
            message=f"Archivo multimedia {file.filename} subido exitosamente",
            data={
                "file_path": file_path,
                "filename": file.filename,
                "media_type": media_type,
                "file_size": len(content),
                "file_extension": file_extension
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/multimedia/available-methods", response_model=APIResponse)
async def get_available_multimedia_methods():
    """
    Obtener métodos de extracción de características disponibles según las dependencias instaladas
    """
    try:
        available_methods = {
            "image": [],
            "audio": []
        }
        
        # Verificar métodos de imagen disponibles
        try:
            from multimedia.feature_extractors.image_extractor import ImageFeatureExtractor
            available_methods["image"] = ImageFeatureExtractor.get_available_methods()
        except ImportError:
            pass
        
        # Verificar métodos de audio disponibles
        try:
            from multimedia.feature_extractors.audio_extractor import AudioFeatureExtractor
            available_methods["audio"] = AudioFeatureExtractor.get_available_methods()
        except ImportError:
            pass
        
        return APIResponse(
            success=True,
            message="Métodos de extracción de características disponibles",
            data=available_methods
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== ENDPOINTS TRADICIONALES Y TEXTUALES (EXISTENTES) ==========

@app.post("/tables/load-pickle", response_model=APIResponse)
async def load_pickle_table(request: LoadPickleTableRequest):
    """
    Cargar una tabla desde un archivo pickle (.pkl)
    """
    try:
        if not os.path.exists(request.pickle_file_path):
            raise HTTPException(status_code=404, detail=f"Archivo pickle no encontrado: {request.pickle_file_path}")
        
        # Detectar si es parte de un conjunto multimedia (histograms, codebook, features)
        base_path = os.path.dirname(request.pickle_file_path)
        base_name = os.path.basename(request.pickle_file_path)
        
        # Detectar si es un índice SPIMI
        if 'spimi' in base_name.lower() or 'index' in base_name.lower():
            print(f"Detectado archivo de índice: {base_name}")
            
            # Cargar el pickle para ver qué contiene
            with open(request.pickle_file_path, 'rb') as f:
                index_data = pickle.load(f)
            
            # Si es un diccionario con estructura de índice SPIMI
            if isinstance(index_data, dict) and any(key in index_data for key in ['index', 'posting_lists', 'doc_info']):
                print("Estructura de índice SPIMI detectada")
                
                # Extraer información del índice
                doc_info = index_data.get('doc_info', {})
                headers = index_data.get('headers', ['content'])
                
                # Debug: Ver qué hay en el índice
                print(f"Contenido del índice SPIMI:")
                print(f"  - Claves disponibles: {list(index_data.keys())}")
                print(f"  - Headers guardados: {headers}")
                if 'metadata' in index_data:
                    print(f"  - Metadata encontrada: {len(index_data['metadata'])} entradas")
                if 'original_table' in index_data:
                    print(f"  - Tabla original: {index_data['original_table']}")
                
                # Registrar la tabla textual directamente sin CSV
                engine.text_tables[request.table_name] = {
                    'index_path': request.pickle_file_path,
                    'text_fields': headers,
                    'csv_path': None,
                    'type': 'SPIMI',
                    'doc_count': len(doc_info),
                    'term_count': len(index_data.get('index', {})),
                    'metadata': index_data.get('metadata', {})
                }
                
                # Registrar headers
                engine.table_headers[request.table_name] = headers + ['similarity_score']
                
                print(f"Tabla textual '{request.table_name}' registrada exitosamente")
                print(f"  - Documentos: {len(doc_info)}")
                print(f"  - Términos: {len(index_data.get('index', {}))}")
                
                return APIResponse(
                    success=True,
                    message=f"Índice SPIMI cargado exitosamente para tabla '{request.table_name}'",
                    data={
                        "table_name": request.table_name,
                        "index_type": "SPIMI",
                        "index_path": request.pickle_file_path,
                        "doc_count": len(doc_info),
                        "term_count": len(index_data.get('index', {})),
                        "headers": headers
                    }
                )
            else:
                # No es un índice SPIMI, continuar con el proceso normal
                pass
        
        # Detectar si es un archivo multimedia
        elif any(keyword in base_name.lower() for keyword in ['histograms', 'codebook', 'features']):
            # Extraer el prefijo común
            prefix = base_name.lower().replace('_histograms.pkl', '').replace('_codebook.pkl', '').replace('_features.pkl', '')
            
            # Buscar los otros archivos relacionados
            # Primero intentar con el patrón estándar
            histograms_path = os.path.join(base_path, f"{prefix}_histograms.pkl")
            codebook_path = os.path.join(base_path, f"{prefix}_codebook.pkl")
            features_path = os.path.join(base_path, f"{prefix}_features.pkl")
            
            # Si no existen, buscar archivos que contengan estas palabras clave
            if not all(os.path.exists(p) for p in [histograms_path, codebook_path, features_path]):
                import glob
                histograms_files = glob.glob(os.path.join(base_path, "*histograms*.pkl"))
                codebook_files = glob.glob(os.path.join(base_path, "*codebook*.pkl"))
                features_files = glob.glob(os.path.join(base_path, "*features*.pkl"))
                
                if histograms_files and codebook_files and features_files:
                    histograms_path = histograms_files[0]
                    codebook_path = codebook_files[0]
                    features_path = features_files[0]
                    # Actualizar el prefijo basado en los archivos encontrados
                    prefix = os.path.basename(histograms_path).replace('_histograms.pkl', '')
            
            # Si encontramos los 3 archivos, es una tabla multimedia
            if all(os.path.exists(p) for p in [histograms_path, codebook_path, features_path]):
                print(f"Detectado conjunto de archivos multimedia para '{prefix}'")
                
                # Detectar tipo de media
                media_type = 'image'  # Por defecto
                if any(audio_kw in prefix for audio_kw in ['audio', 'sound', 'music', 'fma']):
                    media_type = 'audio'
                elif any(img_kw in prefix for img_kw in ['fashion', 'image', 'photo', 'picture']):
                    media_type = 'image'
                
                # Usar el prefijo limpio como nombre de tabla
                clean_table_name = prefix
                
                # Buscar un archivo CSV asociado
                csv_path = None
                import glob
                
                # Buscar en varios lugares posibles
                search_patterns = [
                    os.path.join(base_path, f"{prefix}*.csv"),
                    os.path.join(os.path.dirname(base_path), f"{prefix}*.csv"),
                    os.path.join(os.path.dirname(base_path), "datos", f"{prefix}*.csv"),
                    os.path.join(os.path.dirname(base_path), "..", "datos", f"{prefix}*.csv"),
                    f"datos/{prefix}*.csv",
                    f"{prefix}*.csv"
                ]
                
                for pattern in search_patterns:
                    matches = glob.glob(pattern)
                    if matches:
                        csv_path = matches[0]
                        break
                
                # Si no encontramos con el prefijo, buscar cualquier CSV de fashion
                if not csv_path and 'fashion' in prefix:
                    for pattern in ["datos/fashion*.csv", "fashion*.csv", "*/fashion*.csv"]:
                        matches = glob.glob(pattern)
                        if matches:
                            csv_path = matches[0]
                            break
                
                print(f"Creando tabla multimedia con nombre: '{clean_table_name}'")
                print(f"  - Histograms: {histograms_path}")
                print(f"  - Codebook: {codebook_path}")
                print(f"  - Features: {features_path}")
                print(f"  - CSV metadata: {csv_path if csv_path else 'No encontrado'}")
                
                # Crear request para multimedia
                multimedia_request = LoadMultimediaFromPicklesRequest(
                    table_name=clean_table_name,  # Usar el nombre limpio
                    histograms_path=histograms_path,
                    codebook_path=codebook_path,
                    features_path=features_path,
                    media_type=media_type,
                    csv_path=csv_path
                )
                
                # Llamar al endpoint multimedia
                return await load_multimedia_from_pickles(multimedia_request)
        
        # Si no es multimedia, continuar con el proceso normal
        # Cargar el archivo pickle
        with open(request.pickle_file_path, 'rb') as f:
            data = pickle.load(f)
        
        # Detectar si es un DataFrame de pandas o una estructura de datos simple
        if hasattr(data, 'to_csv'):  # Es un DataFrame
            # Convertir a CSV temporal
            temp_csv = f"temp_{request.table_name}.csv"
            data.to_csv(temp_csv, index=False)
            
            # Cargar usando el motor existente
            result = engine.load_csv(
                table=request.table_name,
                path=temp_csv,
                tipo=request.index_type,
                index_field=0  # Por defecto usar primera columna
            )
            
            # Limpiar archivo temporal
            if os.path.exists(temp_csv):
                os.remove(temp_csv)
                
            headers = list(data.columns)
            record_count = len(data)
            
        elif isinstance(data, dict):
            # Si es un diccionario con estructura de índice
            if 'embeddings' in data and 'metadata' in data:
                # Es una estructura de embeddings
                embeddings = data['embeddings']
                metadata = data.get('metadata', {})
                
                # Registrar en el engine como tabla de embeddings
                engine.register_embedding_table(
                    table_name=request.table_name,
                    embeddings=embeddings,
                    metadata=metadata,
                    pickle_path=request.pickle_file_path
                )
                
                headers = ['embedding_id'] + list(metadata.keys()) if metadata else ['embedding_id', 'embedding_vector']
                record_count = len(embeddings) if hasattr(embeddings, '__len__') else 0
                result = f"Tabla de embeddings '{request.table_name}' cargada exitosamente desde pickle"
                
            else:
                # Intentar convertir a formato tabular
                raise HTTPException(status_code=400, detail="Formato de pickle no reconocido. Debe ser DataFrame o estructura de embeddings.")
        
        elif isinstance(data, list):
            # Lista de registros
            if not data:
                raise HTTPException(status_code=400, detail="El archivo pickle está vacío")
            
            # Crear CSV temporal
            temp_csv = f"temp_{request.table_name}.csv"
            
            # Detectar headers
            if isinstance(data[0], dict):
                headers = list(data[0].keys())
                with open(temp_csv, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(data)
            else:
                headers = [f"column_{i}" for i in range(len(data[0]))]
                with open(temp_csv, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(data)
            
            # Cargar usando el motor
            result = engine.load_csv(
                table=request.table_name,
                path=temp_csv,
                tipo=request.index_type,
                index_field=0
            )
            
            # Limpiar archivo temporal
            if os.path.exists(temp_csv):
                os.remove(temp_csv)
                
            record_count = len(data)
        
        else:
            raise HTTPException(status_code=400, detail=f"Tipo de datos en pickle no soportado: {type(data)}")
        
        return APIResponse(
            success=True,
            message=result,
            data={
                "table_name": request.table_name,
                "pickle_path": request.pickle_file_path,
                "headers": headers,
                "record_count": record_count,
                "data_type": type(data).__name__
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
        
        print(f"Ejecutando consulta SPIMI: {sql_query}")
        
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
        
        print(f"Ejecutando búsqueda textual: {sql_query}")
        
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

# ========== RESTO DE ENDPOINTS EXISTENTES (simplificados por espacio) ==========

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

@app.post("/tables/upload-pickle-file")
async def upload_pickle_file(file: UploadFile = File(...)):
    """
    Subir un archivo pickle al servidor
    """
    try:
        print(f"Recibiendo archivo: {file.filename}")
        print(f"Content-Type: {file.content_type}")
        
        # Validar extensión
        if not file.filename:
            raise HTTPException(status_code=400, detail="No se proporcionó nombre de archivo")
        
        if not file.filename.endswith('.pkl'):
            raise HTTPException(status_code=400, detail=f"Solo se permiten archivos .pkl. Archivo recibido: {file.filename}")
        
        # Crear directorio de datos si no existe
        os.makedirs("datos/pickles", exist_ok=True)
        
        # Guardar archivo subido
        file_path = f"datos/pickles/{file.filename}"
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Intentar cargar el pickle para validar
        try:
            print(f"Intentando cargar pickle desde: {file_path}")
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
                data_type = type(data).__name__
                print(f"Tipo de datos cargado: {data_type}")
                
                # Información básica sobre el contenido
                info = {
                    "data_type": data_type,
                    "file_size": len(content)
                }
                
                if hasattr(data, 'shape'):
                    info["shape"] = str(data.shape)
                    print(f"Shape: {data.shape}")
                elif hasattr(data, '__len__'):
                    info["length"] = len(data)
                    print(f"Length: {len(data)}")
                
        except Exception as e:
            print(f"Error al cargar pickle: {str(e)}")
            import traceback
            traceback.print_exc()
            os.remove(file_path)  # Limpiar archivo inválido
            raise HTTPException(status_code=400, detail=f"Archivo pickle inválido: {str(e)}")
        
        return APIResponse(
            success=True,
            message=f"Archivo pickle {file.filename} subido exitosamente",
            data={
                "file_path": file_path,
                "filename": file.filename,
                **info
            }
        )
    
    except HTTPException:
        raise
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
    try:
        # Verificar tanto tablas tradicionales como textuales
        text_tables = getattr(engine, 'text_tables', {})
        if table_name not in engine.tables and table_name not in text_tables and table_name not in sql_parser.multimedia_engines:
            raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")

        # NOTA: Las tablas textuales y multimedia no soportan scan directo
        if table_name in text_tables:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{table_name}' es textual. Use /search/text para búsquedas semánticas."
            )
        
        if table_name in sql_parser.multimedia_engines:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{table_name}' es multimedia. Use /multimedia/search para búsquedas por similitud."
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

# En api.py, en el endpoint /sql/execute, reemplazar esta sección:

@app.post("/sql/execute", response_model=APIResponse)
async def execute_sql(request: SQLQueryRequest):
    """
    Ejecutar consulta SQL (tradicionales + SPIMI con @@ + multimedia con <->)
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
        
        # Detectar tipo de consulta para mensaje apropiado
        query_type = "unknown"
        if ' <-> ' in request.query:
            query_type = "multimedia_search"
        elif ' @@ ' in request.query:
            query_type = "text_search"
        elif query_lower.startswith('create multimedia'):
            query_type = "create_multimedia_table"
        elif query_lower.startswith('create table') and 'spimi' in query_lower:
            query_type = "create_text_index"
        elif query_lower.startswith('select'):
            query_type = "select"
        elif query_lower.startswith('create'):
            query_type = "create_table"
        
        # Procesar resultado según tipo
        if isinstance(result, dict) and 'results' in result:
            # Resultado multimedia estructurado
            multimedia_results = result['results']
            
            # Determinar columnas basado en los campos solicitados
            query_info = result.get('query_info', {})
            fields = query_info.get('fields', ['*'])
            
            # Construir columnas y filas basado en los campos solicitados
            if fields == ['*']:
                # Para SELECT *, incluir todos los campos disponibles
                if multimedia_results:
                    first_result = multimedia_results[0]
                    metadata_keys = list(first_result.get('metadata', {}).keys())
                    columns = ['rank', 'filename', 'similarity'] + metadata_keys
                    # Asegurar que audio_path esté presente para búsquedas de audio
                    if query_type == "multimedia_search" and table_name and "audio" in table_name.lower():
                        if 'audio_path' not in columns:
                            columns.append('audio_path')
                else:
                    columns = ['rank', 'filename', 'similarity']
            else:
                # Para campos específicos
                columns = fields.copy()
                if 'similarity' not in columns:
                    columns.append('similarity')
                # Asegurar que audio_path esté presente para búsquedas de audio
                if query_type == "multimedia_search" and table_name and "audio" in table_name.lower():
                    if 'audio_path' not in columns:
                        columns.append('audio_path')
                # Asegurar que image_path esté presente para búsquedas de imágenes
                elif query_type == "multimedia_search" and table_name and ("fashion" in table_name.lower() or "image" in table_name.lower()):
                    if 'image_path' not in columns:
                        columns.append('image_path')
            
            # Construir filas con todos los datos
            rows = []
            for res in multimedia_results:
                row = []
                metadata = res.get('metadata', {})
                
                for col in columns:
                    if col == 'rank':
                        row.append(str(res.get('rank', '')))
                    elif col == 'filename':
                        row.append(res.get('filename', ''))
                    elif col == 'similarity':
                        row.append(str(res.get('similarity', 0)))
                    elif col == 'file_path':
                        row.append(res.get('file_path', ''))
                    elif col == 'audio_path':
                        # Para audio_path, usar el valor de metadata o file_path como fallback
                        audio_path = metadata.get('audio_path', res.get('file_path', ''))
                        row.append(audio_path)
                    elif col == 'image_path':
                        # Para image_path, usar el valor de metadata o file_path como fallback
                        image_path = metadata.get('image_path', res.get('file_path', ''))
                        row.append(image_path)
                    else:
                        # Buscar en metadata
                        row.append(str(metadata.get(col, '')))
                
                rows.append(row)
            
            parsed_data = {
                "columns": columns,
                "rows": rows,
                "count": len(multimedia_results)
            }
            
            return APIResponse(
                success=True,
                message=f"Consulta {query_type} ejecutada exitosamente en {result.get('execution_time', execution_time):.4f}s",
                data={
                    "columns": parsed_data["columns"],    
                    "rows": parsed_data["rows"],          
                    "count": parsed_data["count"], 
                    "query": request.query,
                    "query_type": query_type,
                    "execution_time": result.get('execution_time', execution_time),
                    "table_name": table_name,
                    "multimedia_stats": result.get('stats', {})
                }
            )
        
        # Resto de procesamiento para resultados tradicionales
        if isinstance(result, str):
            parsed_data = {"columns": ["message"], "rows": [[result]], "count": 1}
        elif isinstance(result, list):
            # CORREGIDO: Para búsquedas textuales, construir headers desde la consulta
            headers = []
            
            if ' @@ ' in request.query and table_name:
                # Para búsquedas textuales, el parser ya retorna los datos filtrados correctamente
                # Solo necesitamos extraer los headers de la consulta
                select_match = re.search(r'SELECT\s+(.*?)\s+FROM', request.query, re.IGNORECASE)
                if select_match:
                    fields_str = select_match.group(1).strip()
                    if fields_str == '*':
                        # Para SELECT *, usar headers originales + similarity_score
                        headers = engine.get_table_headers(table_name).copy()
                        headers.append("similarity_score")
                    else:
                        # Para campos específicos, usar exactamente los campos solicitados + similarity_score
                        headers = [f.strip() for f in fields_str.split(',')]
                        headers.append("similarity_score")
                else:
                    # Fallback
                    headers = engine.get_table_headers(table_name).copy()
                    headers.append("similarity_score")
            else:
                # Para consultas tradicionales, usar headers de la tabla
                if table_name:
                    headers = engine.get_table_headers(table_name)
                else:
                    headers = ["column_1"]
            
            parsed_data = parse_csv_records(result, headers)
        else:
            parsed_data = {"columns": ["result"], "rows": [[str(result)]], "count": 1}
            
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
                "table_name": table_name
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables", response_model=APIResponse)
async def list_tables():
    """
    Listar todas las tablas disponibles (tradicionales + textuales + multimedia + embeddings)
    """
    try:
        all_tables_info = {}
        
        # Tablas tradicionales
        for table_name, index in engine.tables.items():
            # Skip tablas multimedia que también están en engine.tables
            if table_name in sql_parser.multimedia_engines:
                continue
                
            table_info = engine.get_table_info(table_name)
            all_tables_info[table_name] = {
                "type": "traditional",
                "index_type": type(index).__name__ if hasattr(index, '__name__') else str(type(index)),
                "field_index": getattr(index, 'field_index', None),
                "headers": table_info.get('headers', []),
                "headers_count": len(table_info.get('headers', [])),
                "csv_path": table_info.get('csv_path', ''),
                "record_count": table_info.get('record_count', 0)
            }
        
        # Tablas textuales
        text_tables = getattr(engine, 'text_tables', {})
        for table_name, info in text_tables.items():
            headers = engine.get_table_headers(table_name)
            all_tables_info[table_name] = {
                "type": "textual",
                "index_type": "SPIMI",
                "text_fields": info.get('text_fields', []),
                "headers": headers,
                "headers_count": len(headers),
                "csv_path": info.get('csv_path', ''),
                "index_path": info.get('index_path', ''),
                "record_count": info.get('record_count', 0)
            }
        
        # Tablas multimedia
        for table_name in sql_parser.multimedia_engines.keys():
            multimedia_info = sql_parser.get_multimedia_table_info(table_name)
            headers = multimedia_info.get('headers', [])
            all_tables_info[table_name] = {
                "type": "multimedia",
                "index_type": f"Multimedia_{multimedia_info.get('media_type', 'unknown')}",
                "media_type": multimedia_info.get('media_type'),
                "feature_method": multimedia_info.get('feature_method'),
                "n_clusters": multimedia_info.get('n_clusters'),
                "headers": headers,
                "headers_count": len(headers),
                "csv_path": multimedia_info.get('csv_path', ''),
                "features_extracted": multimedia_info.get('features_extracted', 0),
                "is_built": multimedia_info.get('is_built', False),
                "record_count": multimedia_info.get('record_count', multimedia_info.get('features_extracted', 0))
            }
        
        # Tablas de embeddings
        embedding_tables = getattr(engine, 'embedding_tables', {})
        for table_name, info in embedding_tables.items():
            headers = engine.get_table_headers(table_name)
            embeddings = info.get('embeddings')
            all_tables_info[table_name] = {
                "type": "embeddings",
                "index_type": "Embeddings",
                "headers": headers,
                "headers_count": len(headers),
                "pickle_path": info.get('pickle_path', ''),
                "embeddings_shape": embeddings.shape if hasattr(embeddings, 'shape') else 'unknown',
                "record_count": len(embeddings) if hasattr(embeddings, '__len__') else 0
            }
        
        return APIResponse(
            success=True,
            message=f"Se encontraron {len(all_tables_info)} tablas ({len(engine.tables) - len(sql_parser.multimedia_engines)} tradicionales, {len(text_tables)} textuales, {len(sql_parser.multimedia_engines)} multimedia, {len(embedding_tables)} embeddings)",
            data={
                "tables": all_tables_info, 
                "count": len(all_tables_info),
                "traditional_count": len(engine.tables) - len(sql_parser.multimedia_engines),
                "textual_count": len(text_tables),
                "multimedia_count": len(sql_parser.multimedia_engines),
                "embeddings_count": len(embedding_tables)
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== ENDPOINTS ADICIONALES PARA COMPLETITUD ==========

@app.post("/records/search", response_model=APIResponse)
async def search_records(request: SearchRequest):
    """
    Buscar registros por clave exacta (solo tablas tradicionales)
    """
    try:
        # Verificar que no sea tabla textual o multimedia
        text_tables = getattr(engine, 'text_tables', {})
        if request.table_name in text_tables:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{request.table_name}' es textual. Use /search/text para búsquedas semánticas."
            )
        
        if request.table_name in sql_parser.multimedia_engines:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{request.table_name}' es multimedia. Use /multimedia/search para búsquedas por similitud."
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

@app.delete("/records/delete", response_model=APIResponse)
async def delete_records(request: DeleteRequest):
    """
    Eliminar registros por clave (solo tablas tradicionales)
    """
    try:
        # Verificar que no sea tabla textual o multimedia
        text_tables = getattr(engine, 'text_tables', {})
        if request.table_name in text_tables:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{request.table_name}' es textual. Eliminación no soportada para índices SPIMI."
            )
        
        if request.table_name in sql_parser.multimedia_engines:
            raise HTTPException(
                status_code=400, 
                detail=f"Tabla '{request.table_name}' es multimedia. Eliminación no soportada."
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)