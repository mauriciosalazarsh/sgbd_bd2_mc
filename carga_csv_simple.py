# carga_csv_simple.py
from engine import Engine

def cargar_csv_simple(archivo_csv, nombre_tabla="mi_tabla"):
    """
    Función simple para cargar un CSV con configuración por defecto
    """
    engine = Engine()
    
    try:
        # Cargar con configuración por defecto:
        # - Índice B+Tree (más versátil)
        # - Campo 0 como índice (primera columna)
        resultado = engine.load_csv(
            table=nombre_tabla,
            path=archivo_csv,
            tipo='bplustree',  # Índice recomendado
            index_field=0      # Primera columna
        )
        
        print(f"✅ {resultado}")
        
        # Mostrar información de la tabla
        headers = engine.get_table_headers(nombre_tabla)
        print(f"📋 Columnas detectadas: {headers}")
        
        # Mostrar algunos registros de ejemplo
        registros = engine.scan(nombre_tabla)
        lineas = registros.split('\n')[:3]  # Primeros 3 registros
        print(f"📄 Primeros registros:")
        for i, linea in enumerate(lineas, 1):
            print(f"  {i}: {linea}")
            
        return engine, nombre_tabla
        
    except Exception as e:
        print(f"❌ Error cargando CSV: {e}")
        return None, None

# Ejemplo de uso
if __name__ == "__main__":
    # Cambiar por tu archivo CSV
    archivo = "datos/mi_archivo.csv"
    
    engine, tabla = cargar_csv_simple(archivo, "mi_tabla")
    
    if engine:
        print(f"\n🎉 Tabla '{tabla}' cargada exitosamente!")
        print("Puedes hacer consultas como:")
        print(f"  - engine.scan('{tabla}')  # Ver todos los registros")
        print(f"  - engine.search('{tabla}', 'valor', 0)  # Buscar en columna 0")