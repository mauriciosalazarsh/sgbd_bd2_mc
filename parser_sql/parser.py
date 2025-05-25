# parser_sql.py — Parser SQL personalizado para proyecto multimodal

import re

class ParserSQL:
    def __init__(self):
        self.tables = {}  # nombre_tabla -> definición de campos y estructura

    def parse(self, query):
        query = query.strip().lower()
        if query.startswith("create table"):
            return self._parse_create(query)
        elif query.startswith("insert into"):
            return self._parse_insert(query)
        elif query.startswith("select"):
            return self._parse_select(query)
        elif query.startswith("delete from"):
            return self._parse_delete(query)
        elif query.startswith("load") or query.startswith("from file"):
            return self._parse_load(query)
        else:
            raise ValueError("Consulta no reconocida")

    def _parse_create(self, query):
        # Ejemplo: CREATE TABLE restaurantes (id int, nombre varchar, coords array[float] index rtree)
        match = re.match(r"create table (\w+)\s*\((.+)\)", query)
        if not match:
            raise ValueError("Sintaxis inválida en CREATE")
        table, fields = match.groups()
        columns = [col.strip() for col in fields.split(',')]
        parsed_columns = []
        for col in columns:
            parts = col.split()
            name = parts[0]
            coltype = parts[1]
            index_type = parts[3].lower() if len(parts) == 4 else None
            parsed_columns.append({"name": name, "type": coltype, "index": index_type})
        self.tables[table] = parsed_columns
        return {"action": "create", "table": table, "columns": parsed_columns}

    def _parse_insert(self, query):
        # Ejemplo: insert into tabla values (1, "juan", [5.6, 3.2])
        match = re.match(r"insert into (\w+) values \((.+)\)", query)
        if not match:
            raise ValueError("Sintaxis inválida en INSERT")
        table, values = match.groups()
        parsed_values = [eval(v.strip()) for v in values.split(',')]
        return {"action": "insert", "table": table, "values": parsed_values}

    def _parse_select(self, query):
        # Ejemplo: select * from tabla where nombre between a and b
        pattern = r"select \* from (\w+)(?: where (.+))?"
        match = re.match(pattern, query)
        if not match:
            raise ValueError("Sintaxis inválida en SELECT")
        table, condition = match.groups()
        return {"action": "select", "table": table, "condition": condition}

    def _parse_delete(self, query):
        # Ejemplo: delete from tabla where id = 5
        match = re.match(r"delete from (\w+) where (.+)", query)
        if not match:
            raise ValueError("Sintaxis inválida en DELETE")
        table, condition = match.groups()
        return {"action": "delete", "table": table, "condition": condition}

    def _parse_load(self, query):
        # Ejemplo: from file "data.csv" using index isam("id")
        match = re.search(r"from file \"(.+?)\" using index (\w+)\((.+?)\)", query)
        if not match:
            raise ValueError("Sintaxis inválida en LOAD FILE")
        filepath, index_type, field = match.groups()
        return {"action": "load", "file": filepath, "index": index_type.lower(), "field": field.strip()}