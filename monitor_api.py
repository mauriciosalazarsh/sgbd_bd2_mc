#!/usr/bin/env python3
"""
Monitor para ver el estado de tu API durante el benchmark
"""

import requests
import time
import sys
from datetime import datetime

def monitor_api(api_url="http://localhost:8000", interval=30):
    """Monitorea el estado de la API"""
    
    print("🔍 MONITOR DE TU API MULTIMEDIA")
    print("=" * 50)
    print(f"🔗 URL: {api_url}")
    print(f"⏱️ Intervalo: {interval}s")
    print("Press Ctrl+C to stop")
    print()
    
    try:
        while True:
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            try:
                # Health check
                response = requests.get(f"{api_url}/health", timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    health_data = data.get('data', {})
                    
                    status = health_data.get('status', 'unknown')
                    multimedia_tables = health_data.get('multimedia_tables', [])
                    total_tables = health_data.get('total_tables', 0)
                    
                    print(f"[{timestamp}] ✅ API Status: {status}")
                    print(f"           📊 Total tables: {total_tables}")
                    
                    if multimedia_tables:
                        print(f"           🎯 Multimedia tables: {len(multimedia_tables)}")
                        for table in multimedia_tables[-3:]:  # Últimas 3 tablas
                            print(f"              - {table}")
                    else:
                        print(f"           🎯 No multimedia tables")
                    
                    # Info de tablas multimedia
                    if multimedia_tables:
                        latest_table = multimedia_tables[-1]
                        try:
                            table_response = requests.get(
                                f"{api_url}/multimedia/tables/{latest_table}/info", 
                                timeout=5
                            )
                            if table_response.status_code == 200:
                                table_info = table_response.json()['data']
                                features = table_info.get('features_extracted', 'N/A')
                                is_built = table_info.get('is_built', False)
                                print(f"              ├─ Features: {features}, Built: {is_built}")
                        except:
                            pass
                else:
                    print(f"[{timestamp}] ❌ API Error: {response.status_code}")
                    
            except requests.exceptions.Timeout:
                print(f"[{timestamp}] ⏰ API Timeout")
                
            except requests.exceptions.ConnectionError:
                print(f"[{timestamp}] 🔌 API Connection Error")
                
            except Exception as e:
                print(f"[{timestamp}] ❌ Error: {e}")
            
            print()
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n👋 Monitor detenido")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        api_url = sys.argv[1]
    else:
        api_url = "http://localhost:8000"
        
    if len(sys.argv) > 2:
        interval = int(sys.argv[2])
    else:
        interval = 30
    
    monitor_api(api_url, interval)