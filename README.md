# SQL To OData

[![Tests](https://github.com/lordjabez/sql-to-odata/actions/workflows/test.yml/badge.svg)](https://github.com/lordjabez/sql-to-odata/actions/workflows/test.yml)
[![Linter](https://github.com/lordjabez/sql-to-odata/actions/workflows/lint.yml/badge.svg)](https://github.com/lordjabez/sql-to-odata/actions/workflows/lint.yml)
[![Security](https://github.com/lordjabez/sql-to-odata/actions/workflows/scan.yml/badge.svg)](https://github.com/lordjabez/sql-to-odata/actions/workflows/scan.yml)
[![Release](https://github.com/lordjabez/sql-to-odata/actions/workflows/release.yml/badge.svg)](https://github.com/lordjabez/sql-to-odata/actions/workflows/release.yml)

This Python package provides tools to facilitate adding an OData interface in front of a SQL database.


## Prerequisites

Installation is via `pip`:

```bash
pip install sql-to-odata
```


## Usage

Basic usage is as follows:

```python3
import sql_to_odata

odata_interface = sql_to_odata.ODataInterface(domain_name='example.com', sqlite_filename='my.db')

schema_xml = odata_interface.get_database_schema_xml()
table_json = odata_interface.get_table_json('mytable')
```

TODO write usage description here


## References

*  https://www.sqlitetutorial.net/sqlite-sample-database/
