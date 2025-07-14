#!/usr/bin/env python3
"""
Script para ejecutar benchmark escalable con monitoreo de progreso
"""

import subprocess
import sys
import time
import os
from datetime import datetime

def run_scalable_benchmark():
    """Ejecuta el benchmark escalable con monitoreo"""
    
    print("🚀 INICIANDO BENCHMARK ESCALABLE DE TU API MULTIMEDIA")
    print("=" * 80)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Datasets y tamaños esperados
    datasets = [
        ("Fashion (imágenes)", [1000, 2000, 4000, 8000, 16000, 32000, 44000]),
        ("FMA (audio)", [1000, 2000, 4000, 8000, 16000, 25000])
    ]
    
    total_tasks = sum(len(sizes) for _, sizes in datasets)
    print(f"📊 Total de experimentos planificados: {total_tasks}")
    print()
    
    for dataset_name, sizes in datasets:
        print(f"📁 {dataset_name}: {len(sizes)} tamaños - {sizes}")
    
    print("\n" + "="*80)
    print("💡 ESTIMACIONES DE TIEMPO:")
    print("   • 1K muestras: ~30s construcción + ~5s búsquedas")
    print("   • 8K muestras: ~3-5 min construcción + ~10s búsquedas") 
    print("   • 32K muestras: ~10-15 min construcción + ~30s búsquedas")
    print("   • 64K muestras: ~20-30 min construcción + ~60s búsquedas")
    print("   ⏱️ Tiempo total estimado: 3-5 horas")
    print("="*80)
    
    # Confirmar ejecución
    response = input("\n¿Continuar con el benchmark completo? (y/N): ").strip().lower()
    if response not in ['y', 'yes', 'sí', 'si']:
        print("❌ Benchmark cancelado por el usuario")
        return False
    
    print("\n🏃‍♂️ Iniciando benchmark escalable...")
    print("💾 Los resultados se guardarán automáticamente en JSON")
    print("🔄 Puedes ver el progreso en tiempo real...")
    print()
    
    try:
        # Ejecutar el benchmark principal
        cmd = ['python', 'multimedia_knn_benchmark.py']
        
        # Usar Popen para capturar output en tiempo real
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            stdin=subprocess.PIPE
        )
        
        # Enviar enter para usar URL por defecto
        process.stdin.write('\n')
        process.stdin.flush()
        
        # Mostrar output en tiempo real
        for line in iter(process.stdout.readline, ''):
            print(line.rstrip())
            sys.stdout.flush()
        
        # Esperar a que termine
        return_code = process.wait()
        
        if return_code == 0:
            print("\n🎉 ¡Benchmark completado exitosamente!")
            
            # Buscar archivo de resultados
            import glob
            result_files = glob.glob("your_multimedia_api_benchmark_*.json")
            if result_files:
                latest_file = max(result_files, key=os.path.getctime)
                print(f"📁 Resultados guardados en: {latest_file}")
            
            return True
        else:
            print(f"\n❌ Benchmark terminó con error (código: {return_code})")
            return False
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Benchmark interrumpido por el usuario")
        print("💾 Los resultados parciales pueden haberse guardado")
        return False
        
    except Exception as e:
        print(f"\n❌ Error ejecutando benchmark: {e}")
        return False

def show_quick_test_option():
    """Muestra opción de test rápido"""
    print("\n💡 ALTERNATIVA: Test rápido")
    print("   Si prefieres un test más rápido (5-10 min), ejecuta:")
    print("   python quick_benchmark.py")
    print()

if __name__ == "__main__":
    show_quick_test_option()
    
    success = run_scalable_benchmark()
    
    if success:
        print("\n✅ Proceso completado. Revisa los archivos JSON para resultados detallados.")
    else:
        print("\n⚠️ Proceso incompleto. Puedes intentar de nuevo o usar el test rápido.")
        show_quick_test_option()