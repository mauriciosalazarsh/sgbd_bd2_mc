#!/usr/bin/env python3
"""
Script de prueba simple para verificar que todo funciona
"""

def test_imports():
    """Prueba que todos los imports funcionen"""
    print("🧪 Probando imports...")
    
    try:
        from engine import Engine
        print("✅ Engine importado correctamente")
    except Exception as e:
        print(f"❌ Error importando Engine: {e}")
        return False
        
    try:
        from parser_sql.parser import SQLParser
        print("✅ SQLParser importado correctamente")
    except Exception as e:
        print(f"❌ Error importando SQLParser: {e}")
        return False
        
    return True

def test_basic_functionality():
    """Prueba funcionalidad básica"""
    print("\n🔧 Probando funcionalidad básica...")
    
    try:
        from engine import Engine
        from parser_sql.parser import SQLParser
        
        engine = Engine()
        parser = SQLParser(engine)
        
        print("✅ Engine y Parser creados correctamente")
        
        # Verificar archivos de datos
        import os
        data_files = [
            "datos/StudentsPerformance.csv",
            "datos/powerplants.csv", 
            "datos/kcdatahouse.csv"
        ]
        
        found_files = []
        for file in data_files:
            if os.path.exists(file):
                found_files.append(file)
                print(f"✅ Archivo encontrado: {file}")
        
        if not found_files:
            print("⚠️  No se encontraron archivos CSV en 'datos/'")
            return False
        
        print(f"✅ Se encontraron {len(found_files)} archivos de datos")
        return True
        
    except Exception as e:
        print(f"❌ Error en prueba básica: {e}")
        return False

def main():
    print("🚀 Sistema de Pruebas Básicas")
    print("=" * 40)
    
    if not test_imports():
        print("\n❌ Error en imports. Verifica que los archivos estén creados.")
        return
    
    if not test_basic_functionality():
        print("\n❌ Error en funcionalidad básica.")
        return
    
    print("\n🎉 ¡Todas las pruebas básicas pasaron!")
    print("\nAhora puedes probar:")
    print("  python main.py interactive")
    print("  python main.py api")

if __name__ == "__main__":
    main()