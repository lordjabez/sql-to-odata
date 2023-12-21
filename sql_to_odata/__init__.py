import logging
import os
import sqlite3

import ujson


__version__ = '0.0.0'


_log = logging.getLogger(__name__)
_log.addHandler(logging.NullHandler())


class ODataInterface():
    """OData interface to a SQL database."""

    @staticmethod
    def datatype_to_odata(type_name):
        """
        Convert a SQLite datatype to an OData type.

        :param type_name: SQLite datatype
        :return: OData type name
        """
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

    def get_table_names(self):
        """
        Fetch the names of all tables in the database.

        :return: List of table names in sorted order
        """
        _log.debug('Fetching all table names')
        query = '''SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';'''
        rows = self._connection.execute(query)
        return sorted(r[0] for r in rows)

    def get_table_schema(self, table_name):
        """
        Fetch a single table's schema.

        :param table_name: Name of the table whose schema should be fetched
        :return: List of tuples of schema info: (name, odata_type, is_nullable, default_value, is_primary_key)
        """
        _log.debug(f'Fetching table schema from {table_name}')
        query = '''SELECT * FROM pragma_table_info(?);'''
        rows = self._connection.execute(query, [table_name])
        return [(r[1], ODataInterface.datatype_to_odata(r[2]), r[3] == 1, r[4], r[5] == 1) for r in rows]

    def get_table_schema_xml(self, table_name):
        """
        Create an OData metadata fragment for the given table in XML format.

        :param table_name: Name of the table whose schema should be fetched
        :return: OData metadata fragment as an XML string
        """
        _log.debug(f'Fetching table schema from {table_name}')
        schema = self.get_table_schema(table_name)
        xml_lines = []
        xml_lines.append(f'<EntityType Name="{table_name}">')
        for field in schema:
            xml_lines.append(f'<Property Name="{field[0]}" Type="{field[1]}" />')
        xml_lines.append('</EntityType>')
        return '\n'.join(xml_lines)

    def get_database_service_json(self, tables_to_include=None, formatted=False):
        """
        Create an OData service document for the database.

        :param tables_to_include: Optional list of tables to include in the document, defaults to all
        :param formatted: JSON output is formatted with indentation, defaults to false
        :return: OData service document in JSON format
        """
        _log.debug('Creating OData service document for database')
        table_names = self.get_table_names() if tables_to_include is None else tables_to_include
        service_json = {'@context': '$metadata'}
        service_json['value'] = [{'name': t, 'kind': 'EntitySet', 'url': t} for t in table_names]
        if formatted:
            return ujson.dumps(service_json, indent=4)
        else:
            return ujson.dumps(service_json, separators=(',', ':'))

    def get_database_schema(self, tables_to_include=None):
        """
        Fetch the schemas for all tables in the database.

        :param tables_to_include: Optional list of tables to include in schema, defaults to all
        :return: Dictionary of schemas, where the keys are the table names and the
                 values are schema tuples as defined in the get_table_schema function.
        """
        _log.debug('Fetching all database table schemas')
        table_names = self.get_table_names() if tables_to_include is None else tables_to_include
        return {t: self.get_table_schema(t) for t in table_names}

    def get_database_schema_xml(self, tables_to_include=None):
        """
        Create an OData metadata file for the database in XML format.

        :param tables_to_include: Optional list of tables to include in schema, defaults to all
        :return: OData metadata as an XML string
        """
        _log.debug('Creating OData metadata XML')
        table_names = self.get_table_names() if tables_to_include is None else tables_to_include
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
        """
        Fetch all rows of a single table.

        :param table_name: Name of the table to be fetched
        :return: List of rows each of which is a dictionary of field name / value pairs
        """
        _log.debug(f'Fetching all rows of table {table_name}')
        table_names = self.get_table_names()
        # Prevents SQL injection by validating parameter against list of table names
        if table_name not in table_names:
            raise ValueError(f'Table not found: {table_name}')
        query = f'''SELECT * FROM {table_name}'''  # nosec - this is safe because of the prior check
        rows = self._connection.execute(query)
        schema = self.get_table_schema(table_name)
        field_names = [f[0] for f in schema]
        return [dict(zip(field_names, r)) for r in rows]

    def get_table_json(self, table_name, formatted=False):
        """
        Fetch all rows of a single table in OData-compatible JSON format.

        :param table_name: Name of the table to be fetched
        :param formatted: JSON output is formatted with indentation, defaults to false
        :return: JSON-formatted rows and an OData context header pointing to metadata
        """
        _log.debug(f'Fetching all rows of table {table_name} in JSON format')
        odata_context_url = f'$metadata#{table_name}'
        table_rows = self.get_table_rows(table_name)
        table_json = {'@odata.context': odata_context_url, 'value': table_rows}
        if formatted:
            return ujson.dumps(table_json, indent=4)
        else:
            return ujson.dumps(table_json, separators=(',', ':'))

    def dump_database(self, folder_name, tables_to_include=None, formatted=False):
        """
        Create a service document, metadata file for the database schemas, and a JSON
        file for each table, suitable for creating an OData-compatible API endpoint.

        :param folder_name: Location to store output files
        :param tables_to_include: Optional list of tables to include in dump, defaults to all
        :param formatted: JSON output is formatted with indentation, defaults to false
        """
        _log.debug(f'Dumping entire database to {folder_name}')
        table_names = self.get_table_names() if tables_to_include is None else tables_to_include
        os.makedirs(folder_name, exist_ok=True)
        service_filename = os.path.join(folder_name, '$service')
        service_json = self.get_database_service_json(tables_to_include)
        schema_filename = os.path.join(folder_name, '$metadata')
        schema_xml = self.get_database_schema_xml()
        with open(service_filename, 'w') as service_file:
            service_file.write(service_json)
        with open(schema_filename, 'w') as schema_file:
            schema_file.write(schema_xml)
        for table_name in table_names:
            table_filename = os.path.join(folder_name, table_name)
            table_json = self.get_table_json(table_name, formatted)
            with open(table_filename, 'w') as table_file:
                table_file.write(table_json)

    def __init__(self, *args, **kwargs):
        """Construct an instance of the class."""
        sqlite_filename = kwargs['sqlite_filename']
        sqlite_uri = f'file:{sqlite_filename}?mode=ro'
        self._connection = sqlite3.connect(sqlite_uri, uri=True)
        _log.debug(f'Opened read-only connection to database at {sqlite_filename}')
