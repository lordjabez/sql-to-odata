import os
import sqlite3

import ujson


__version__ = '0.0.0'


class ODataInterface():
    """OData interface to a SQL database."""

    @staticmethod
    def datatype_to_odata(type_name):
        """TODO"""
        if type_name == 'INTEGER':
            return 'Edm.Int64'
        elif type_name == 'REAL':
            return 'Edm.Double'
        elif type_name.startswith('NUMERIC'):
            return 'Edm.Decimal'
        elif type_name == 'TEXT':
            return 'Edm.String'
        elif type_name.startswith('NVARCHAR'):
            return 'Edm.String'
        elif type_name == 'BLOB':
            return 'Edm.Binary'
        elif type_name == 'DATETIME':
            return 'Edm.DateTimeOffset'
        else:
            raise ValueError(f'Unknown data type: {type_name}')

    def _execute_query(self, query, values=None):
        if values is None:
            values = []
        elif type(values) not in (list, tuple):
            values = [values]
        return self._connection.execute(query, values)

    def get_table_names(self):
        """
        Get list of table names in database.

        :return: List of table names in sorted order
        """
        query = '''SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';'''
        rows = self._execute_query(query)
        return sorted(r[0] for r in rows)

    def get_table_schema(self, table_name):
        """TODO"""
        query = '''SELECT * FROM pragma_table_info(?);'''
        rows = self._execute_query(query, table_name)
        return [(r[1], ODataInterface.datatype_to_odata(r[2]), r[3] == 1, r[4], r[5] == 1) for r in rows]

    def get_table_schema_xml(self, table_name):
        """
        Get an OData schema fragment for the given table.

        :return: Schema as an XML string
        """
        # TODO add support for defaults, not null, and primary key
        schema = self.get_table_schema(table_name)
        xml_lines = []
        xml_lines.append(f'<EntityType Name="{table_name}">')
        for field in schema:
            xml_lines.append(f'<Property Name="{field[0]}" Type="{field[1]}" />')
        xml_lines.append('</EntityType>')
        return '\n'.join(xml_lines)

    def get_database_schema(self):
        return {t: self.get_table_schema(t) for t in self.get_table_names()}

    def get_database_schema_xml(self):
        """TODO"""
        table_names = self.get_table_names()
        xml_lines = []
        xml_lines.append('<?xml version="1.0" encoding="utf-8"?>')
        xml_lines.append('<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">')
        xml_lines.append('<edmx:DataServices>')
        xml_lines.append('<Schema Namespace="DCT" xmlns="http://docs.oasis-open.org/odata/ns/edm">')
        for table_name in table_names:
            xml_lines.append(self.get_table_schema_xml(table_name))
        xml_lines.append('</Schema>')
        xml_lines.append('</edmx:DataServices>')
        xml_lines.append('</edmx:Edmx>')
        return '\n'.join(xml_lines)

    def get_table_rows(self, table_name):
        """TODO"""
        table_names = self.get_table_names()
        # Prevents SQL injection by validating parameter against list of table names
        if table_name not in table_names:
            raise ValueError(f'Table not found: {table_name}')
        query = f'''SELECT * FROM {table_name}'''  # nosec - this is safe because of the prior check
        rows = self._execute_query(query)
        schema = self.get_table_schema(table_name)
        field_names = [f[0] for f in schema]
        return [dict(zip(field_names, r)) for r in rows]

    def get_table_json(self, table_name, formatted=False):
        """TODO"""
        odata_context_url = f'$metadata#{table_name}'
        table_rows = self.get_table_rows(table_name)
        table_json = {'@odata.context': odata_context_url, 'value': table_rows}
        if formatted:
            return ujson.dumps(table_json, indent=4)
        else:
            return ujson.dumps(table_json, separators=(',', ':'))

    def dump_database(self, folder, formatted=False):
        """TODO"""
        os.makedirs(folder, exist_ok=True)
        schema_filename = os.path.join(folder, '$metadata')
        schema_xml = self.get_database_schema_xml()
        with open(schema_filename, 'w') as schema_file:
            schema_file.write(schema_xml)
        table_names = self.get_table_names()
        for table_name in table_names:
            table_filename = os.path.join(folder, table_name)
            table_json = self.get_table_json(table_name, formatted)
            with open(table_filename, 'w') as table_file:
                table_file.write(table_json)

    def __init__(self, *args, **kwargs):
        """Construct an instance of the class."""
        sqlite_filename = kwargs['sqlite_filename']
        sqlite_uri = f'file:{sqlite_filename}?mode=ro'
        self._connection = sqlite3.connect(sqlite_uri, uri=True)
