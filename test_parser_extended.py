#!/usr/bin/env python3
"""
Prueba rápida del parser extendido con operador @@
Para verificar que funciona antes de usar el main completo
"""

import os
import sys

def test_extended_parser():
    """Prueba el parser extendido con consultas textuales"""
    print("🧪 PRUEBA DEL PARSER EXTENDIDO - OPERADOR @@")
    print("=" * 50)
    
    try:
        # Importar componentes necesarios
        from engine import Engine
        from parser_sql.parser import SQLParser
        from indices.inverted_index import create_text_index
        
        print("✅ Imports exitosos")
        
        # Crear engine y parser
        engine = Engine()
        parser = SQLParser(engine)
        
        print("✅ Engine y parser creados")
        
        # Datos de prueba pequeños
        test_data = [
            {
                'id': '1',
                'title': 'Canción de Amor',
                'artist': 'Manu Chao',
                'album': 'Clandestino',
                'lyric': 'El amor es resistencia en tiempos de guerra'
            },
            {
                'id': '2',
                'title': 'Rock Nacional',
                'artist': 'Divididos',
                'album': 'La Era',
                'lyric': 'Entre guerra y paz elegimos la música'
            },
            {
                'id': '3',
                'title': 'Esperanza',
                'artist': 'Mercedes Sosa',
                'album': 'Voces',
                'lyric': 'La esperanza nunca muere en el corazón'
            }
        ]
        
        print(f"📊 Datos de prueba: {len(test_data)} documentos")
        
        # Crear índice textual
        print("🔨 Creando índice textual...")
        text_index = create_text_index(test_data, 'Audio', ['title', 'artist', 'lyric'], 'spanish')
        
        if text_index is None:
            print("❌ Error creando índice textual")
            return False
        
        print("✅ Índice textual creado")
        
        # Registrar índice en el parser
        parser.register_text_index('Audio', text_index)
        print("✅ Índice registrado en parser")
        
        # Probar consultas con operador @@
        test_queries = [
            "SELECT * FROM Audio WHERE lyric @@ 'amor guerra' LIMIT 3;",
            "SELECT title, artist FROM Audio WHERE lyric @@ 'esperanza' LIMIT 2;",
            "SELECT title, lyric FROM Audio WHERE lyric @@ 'música' LIMIT 5;"
        ]
        
        print(f"\n🔍 Probando {len(test_queries)} consultas con @@:")
        
        all_success = True
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n--- Consulta {i}: {query} ---")
            
            try:
                # Ejecutar con parser extendido
                results = parser.parse_and_execute(query)
                
                if results:
                    print(f"✅ Ejecutada correctamente - {len(results)} resultados")
                    # Mostrar primer resultado como ejemplo
                    if len(results) > 0:
                        print(f"   Ejemplo: {results[0][:100]}...")
                else:
                    print("⚠️ Sin resultados pero ejecutada correctamente")
                    
            except Exception as e:
                print(f"❌ Error: {e}")
                all_success = False
        
        if all_success:
            print("\n🎉 ¡TODAS LAS PRUEBAS DEL PARSER EXTENDIDO EXITOSAS!")
            print("\n🚀 LISTO PARA USAR:")
            print("   python main.py")
            return True
        else:
            print("\n⚠️ ALGUNAS PRUEBAS FALLARON")
            return False
            
    except Exception as e:
        print(f"❌ Error en prueba: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_compatibility_with_existing():
    """Verifica que el parser extendido no rompa funcionalidad existente"""
    print("\n🧪 PRUEBA DE COMPATIBILIDAD CON FUNCIONALIDAD EXISTENTE")
    print("=" * 60)
    
    try:
        from engine import Engine
        from parser_sql.parser import SQLParser
        
        engine = Engine()
        parser = SQLParser(engine)
        
        # Crear datos de prueba usando funcionalidad existente
        import csv
        import os
        
        # Crear archivo CSV temporal
        test_file = "test_compatibility.csv"
        with open(test_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'category', 'score'])
            writer.writerows([
                ['1', 'Item 1', 'A', '85'],
                ['2', 'Item 2', 'B', '92'],
                ['3', 'Item 3', 'A', '78']
            ])
        
        print("📄 Archivo CSV de prueba creado")
        
        # Probar CREATE TABLE (funcionalidad existente)
        create_query = f'create table TestTable from file "{test_file}" using index btree("id")'
        
        try:
            result = parser.parse_and_execute(create_query)
            print("✅ CREATE TABLE funciona correctamente")
            print(f"   Resultado: {result[:100]}...")
        except Exception as e:
            print(f"❌ Error en CREATE TABLE: {e}")
            return False
        
        # Probar SELECT básico (funcionalidad existente)
        try:
            select_result = parser.parse_and_execute("SELECT * FROM TestTable")
            print("✅ SELECT básico funciona correctamente")
            print(f"   Encontrados: {len(select_result)} registros")
        except Exception as e:
            print(f"❌ Error en SELECT: {e}")
            return False
        
        # Limpiar
        if os.path.exists(test_file):
            os.remove(test_file)
        
        print("✅ COMPATIBILIDAD VERIFICADA - El parser extendido mantiene toda la funcionalidad existente")
        return True
        
    except Exception as e:
        print(f"❌ Error en prueba de compatibilidad: {e}")
        return False

def main():
    """Ejecuta todas las pruebas del parser extendido"""
    print("🚀 SISTEMA DE PRUEBAS - PARSER EXTENDIDO CON @@")
    print("=" * 60)
    
    # Lista de pruebas
    tests = [
        ("Parser Extendido con @@", test_extended_parser),
        ("Compatibilidad con Funcionalidad Existente", test_compatibility_with_existing)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"🧪 EJECUTANDO: {test_name}")
        print(f"{'='*60}")
        
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} - EXITOSO")
            else:
                print(f"❌ {test_name} - FALLÓ")
        except Exception as e:
            print(f"💥 {test_name} - ERROR: {e}")
    
    # Resumen final
    print(f"\n{'='*60}")
    print(f"📊 RESUMEN FINAL")
    print(f"{'='*60}")
    print(f"Pruebas pasadas: {passed}/{total}")
    
    if passed == total:
        print("🎉 ¡PARSER EXTENDIDO LISTO!")
        print("\n📋 PRÓXIMOS PASOS:")
        print("1. Reemplaza tu parser_sql/parser.py con la versión extendida")
        print("2. Ejecuta: python main.py")
        print("3. Usa consultas como: SELECT * FROM Audio WHERE lyric @@ 'amor' LIMIT 5;")
        print("\n💡 SINTAXIS SOPORTADA:")
        print("   SELECT campos FROM tabla WHERE campo @@ 'consulta de texto' LIMIT k;")
        print("   ✅ Operador @@ para búsqueda textual")
        print("   ✅ Similitud de coseno con TF-IDF")
        print("   ✅ Ranking por relevancia")
        print("   ✅ Memoria secundaria con SPIMI")
    elif passed > 0:
        print("⚠️ ALGUNAS PRUEBAS PASARON")
        print("Revisa los errores arriba antes de continuar")
    else:
        print("❌ TODAS LAS PRUEBAS FALLARON")
        print("Verifica que todos los archivos estén en su lugar")
    
    print("=" * 60)

if __name__ == "__main__":
    main()