from engine import Engine

engine = Engine()

def main():
    while True:
        print("\n--- Sistema de Índices Multimodales ---")
        print("1. Cargar CSV")
        print("2. Insertar Registro")
        print("3. Escanear Tabla")
        print("4. Buscar por key")
        print("5. Buscar por rango")
        print("6. Eliminar por key")
        print("7. Salir")
        opcion = input("Opción: ")

        if opcion == '1':
            table       = input("Nombre de la tabla: ")
            path        = input("Ruta del CSV: ")
            tipo        = input("Tipo de índice (sequential, isam, hash, bplustree, rtree): ")
            index_field = int(input("Columna a indexar (0-n): "))
            try:
                print(engine.load_csv(table, path, tipo, index_field))
            except Exception as e:
                print(f"Error: {e}")

        elif opcion == '2':
            table  = input("Nombre de la tabla: ")
            values = input("Valores separados por coma: ").split(",")
            try:
                print(engine.insert(table, values))
            except Exception as e:
                print(f"Error: {e}")

        elif opcion == '3':
            table = input("Nombre de la tabla: ")
            try:
                print(engine.scan(table))
            except Exception as e:
                print(f"Error: {e}")

        elif opcion == '4':
            table  = input("Nombre de la tabla: ")
            column = int(input("Columna a buscar (0-n): "))
            key    = input("Valor key a buscar: ")
            try:
                resultados = engine.search(table, key, column)
                if resultados:
                    print('\n'.join(resultados))
                else:
                    print("No se encontraron registros.")
            except Exception as e:
                print(f"Error: {e}")

        elif opcion == '5':
            table     = input("Nombre de la tabla: ")
            start_key = input("Clave inicio del rango (lat,lon): ").strip()
            end_key   = input("Clave fin del rango (radio o k): ").strip()
            # validamos que no quede vacío
            while not end_key:
                print("⚠️ Tienes que ingresar un número (float para radio o int para k-NN).")
                end_key = input("Clave fin del rango (radio o k): ").strip()

            # Detectamos tipo de búsqueda
            is_knn = False
            try:
                k = int(end_key)
                is_knn = True
                param = k
            except ValueError:
                param = float(end_key)

            try:
                resultados = engine.range_search(table, start_key, end_key)
                if resultados:
                    # Imprimimos un encabezado
                    if is_knn:
                        print(f"\n=== k-NN search: los {param} más cercanos a {start_key} ===")
                    else:
                        print(f"\n=== Range search: radio={param} alrededor de {start_key} ===")
                    # Y luego cada línea
                    for linea in resultados:
                        print(" ", linea)
                else:
                    print("No se encontraron registros.")
            except Exception as e:
                print(f"Error: {e}")


        elif opcion == '6':
            table = input("Nombre de la tabla: ")
            key   = input("Clave a eliminar: ")
            try:
                print(engine.remove(table, key))
            except Exception as e:
                print(f"Error: {e}")


            
        elif opcion == '7':
            break
        else:
            print("Opción inválida.")

if __name__ == "__main__":
    main()
