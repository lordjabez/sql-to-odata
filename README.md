# SQL To OData

[![Tests](https://github.com/lordjabez/sql-to-odata/actions/workflows/test.yml/badge.svg)](https://github.com/lordjabez/sql-to-odata/actions/workflows/test.yml)
[![Linter](https://github.com/lordjabez/sql-to-odata/actions/workflows/lint.yml/badge.svg)](https://github.com/lordjabez/sql-to-odata/actions/workflows/lint.yml)
[![Security](https://github.com/lordjabez/sql-to-odata/actions/workflows/scan.yml/badge.svg)](https://github.com/lordjabez/sql-to-odata/actions/workflows/scan.yml)
[![Release](https://github.com/lordjabez/sql-to-odata/actions/workflows/release.yml/badge.svg)](https://github.com/lordjabez/sql-to-odata/actions/workflows/release.yml)

This Python package provides tools to facilitate adding an OData interface in front of a SQL
database. It currently only supports extracting SQLite table schemas and data in their entirety
into static files, which can then be hosted on a website or otherwise provided to OData consumers.

It was initially built to create data that can be consumed on [Tableau Public](https://public.tableau.com/).


## Prerequisites

Installation is via `pip`:

```bash
pip install sql-to-odata
```


## Usage

Basic usage is as follows:

```python3
import sql_to_odata

odata_interface = sql_to_odata.ODataInterface(sqlite_filename='stuff.db')

# Extracts the schema for all tables in XML format
schema_xml = odata_interface.get_database_schema_xml()

# Extracts the data from a single table in JSON format
table_json = odata_interface.get_table_json('people')

# Dumps the schemas and all tables to a folder, with the schema
# file named "$metadata" and data files named after the tables;
# this output format can be directly served on a website
odata_interface.dump_database('/path/to/output')

# Dump only a portion of the tables in the database
odata_interface.dump_database('/path/to/output', tables_to_include=['people', 'places', 'things'])
```

Run `help(sql_to_odata)` to get more information on the available functions.


## To-Do

*  Translate settings for nullable fields, default values, and primary keys
*  Create a test database that includes more variety of stuff, esp around datatypes
*  Add tests to ensure mapping of dates and times works (esp since OData requires a TZ-aware datetime)
*  Add support for more source databases


## References

*  [SQLite](https://www.sqlite.org)
*  [OData](https://www.odata.org)
*  [Test Database](https://www.sqlitetutorial.net/sqlite-sample-database)
*  [OData on NodeJS](https://github.com/sachinvprabhu/node-sqlite-odata-cds)
