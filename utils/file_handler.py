# file_handler.py — Módulo para manejar archivos CSV e insertar en índices

import csv

def load_csv(filepath):
    with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
        rows = [row for row in reader]
    return headers, rows

def insert_rows_to_index(headers, rows, index_type, index_instance, key_field):
    if key_field not in headers:
        raise ValueError(f"Campo clave '{key_field}' no encontrado en CSV")

    key_idx = headers.index(key_field)

    for row in rows:
        key = row[key_idx]
        if index_type in ['sequential', 'isam', 'hash', 'bplustree']:
            index_instance.insert(key, row)
        elif index_type == 'rtree':
            # Convertir a vector float si es necesario
            vector = [float(x) for x in row[key_idx].strip('[]').split()] if isinstance(row[key_idx], str) else row[key_idx]
            index_instance.insert(vector, row)
        else:
            raise NotImplementedError(f"Índice '{index_type}' no implementado")