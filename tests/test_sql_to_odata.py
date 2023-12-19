import io
import sqlite3
import zipfile

import pytest
import requests
import toml

import sql_to_odata


_test_domain_name = 'example.com'
_test_sqlite_filename = 'tests/test.db'

_test_table_names = [
    'albums',
    'artists',
    'customers',
    'employees',
    'genres',
    'invoice_items',
    'invoices',
    'media_types',
    'playlist_track',
    'playlists',
    'tracks',
]

_test_table_name = 'albums'

_test_table_schema = [('AlbumId', 'Edm.Int64', True, None, True), ('Title', 'Edm.String', True, None, False), ('ArtistId', 'Edm.Int64', True, None, False)]  # noqa: E501

_test_table_schema_xml = '''<EntityType Name="albums">
<Property Name="AlbumId" Type="Edm.Int64" />
<Property Name="Title" Type="Edm.String" />
<Property Name="ArtistId" Type="Edm.Int64" />
</EntityType>'''

_test_database_schema = {'albums': [('AlbumId', 'Edm.Int64', True, None, True), ('Title', 'Edm.String', True, None, False), ('ArtistId', 'Edm.Int64', True, None, False)], 'artists': [('ArtistId', 'Edm.Int64', True, None, True), ('Name', 'Edm.String', False, None, False)], 'customers': [('CustomerId', 'Edm.Int64', True, None, True), ('FirstName', 'Edm.String', True, None, False), ('LastName', 'Edm.String', True, None, False), ('Company', 'Edm.String', False, None, False), ('Address', 'Edm.String', False, None, False), ('City', 'Edm.String', False, None, False), ('State', 'Edm.String', False, None, False), ('Country', 'Edm.String', False, None, False), ('PostalCode', 'Edm.String', False, None, False), ('Phone', 'Edm.String', False, None, False), ('Fax', 'Edm.String', False, None, False), ('Email', 'Edm.String', True, None, False), ('SupportRepId', 'Edm.Int64', False, None, False)], 'employees': [('EmployeeId', 'Edm.Int64', True, None, True), ('LastName', 'Edm.String', True, None, False), ('FirstName', 'Edm.String', True, None, False), ('Title', 'Edm.String', False, None, False), ('ReportsTo', 'Edm.Int64', False, None, False), ('BirthDate', 'Edm.DateTimeOffset', False, None, False), ('HireDate', 'Edm.DateTimeOffset', False, None, False), ('Address', 'Edm.String', False, None, False), ('City', 'Edm.String', False, None, False), ('State', 'Edm.String', False, None, False), ('Country', 'Edm.String', False, None, False), ('PostalCode', 'Edm.String', False, None, False), ('Phone', 'Edm.String', False, None, False), ('Fax', 'Edm.String', False, None, False), ('Email', 'Edm.String', False, None, False)], 'genres': [('GenreId', 'Edm.Int64', True, None, True), ('Name', 'Edm.String', False, None, False)], 'invoice_items': [('InvoiceLineId', 'Edm.Int64', True, None, True), ('InvoiceId', 'Edm.Int64', True, None, False), ('TrackId', 'Edm.Int64', True, None, False), ('UnitPrice', 'Edm.Decimal', True, None, False), ('Quantity', 'Edm.Int64', True, None, False)], 'invoices': [('InvoiceId', 'Edm.Int64', True, None, True), ('CustomerId', 'Edm.Int64', True, None, False), ('InvoiceDate', 'Edm.DateTimeOffset', True, None, False), ('BillingAddress', 'Edm.String', False, None, False), ('BillingCity', 'Edm.String', False, None, False), ('BillingState', 'Edm.String', False, None, False), ('BillingCountry', 'Edm.String', False, None, False), ('BillingPostalCode', 'Edm.String', False, None, False), ('Total', 'Edm.Decimal', True, None, False)], 'media_types': [('MediaTypeId', 'Edm.Int64', True, None, True), ('Name', 'Edm.String', False, None, False)], 'playlist_track': [('PlaylistId', 'Edm.Int64', True, None, True), ('TrackId', 'Edm.Int64', True, None, False)], 'playlists': [('PlaylistId', 'Edm.Int64', True, None, True), ('Name', 'Edm.String', False, None, False)], 'tracks': [('TrackId', 'Edm.Int64', True, None, True), ('Name', 'Edm.String', True, None, False), ('AlbumId', 'Edm.Int64', False, None, False), ('MediaTypeId', 'Edm.Int64', True, None, False), ('GenreId', 'Edm.Int64', False, None, False), ('Composer', 'Edm.String', False, None, False), ('Milliseconds', 'Edm.Int64', True, None, False), ('Bytes', 'Edm.Int64', False, None, False), ('UnitPrice', 'Edm.Decimal', True, None, False)]}  # noqa: E501

_test_database_schema_xml = '''<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
<edmx:DataServices>
<Schema Namespace="DCT" xmlns="http://docs.oasis-open.org/odata/ns/edm">
<EntityType Name="albums">
<Property Name="AlbumId" Type="Edm.Int64" />
<Property Name="Title" Type="Edm.String" />
<Property Name="ArtistId" Type="Edm.Int64" />
</EntityType>
<EntityType Name="artists">
<Property Name="ArtistId" Type="Edm.Int64" />
<Property Name="Name" Type="Edm.String" />
</EntityType>
<EntityType Name="customers">
<Property Name="CustomerId" Type="Edm.Int64" />
<Property Name="FirstName" Type="Edm.String" />
<Property Name="LastName" Type="Edm.String" />
<Property Name="Company" Type="Edm.String" />
<Property Name="Address" Type="Edm.String" />
<Property Name="City" Type="Edm.String" />
<Property Name="State" Type="Edm.String" />
<Property Name="Country" Type="Edm.String" />
<Property Name="PostalCode" Type="Edm.String" />
<Property Name="Phone" Type="Edm.String" />
<Property Name="Fax" Type="Edm.String" />
<Property Name="Email" Type="Edm.String" />
<Property Name="SupportRepId" Type="Edm.Int64" />
</EntityType>
<EntityType Name="employees">
<Property Name="EmployeeId" Type="Edm.Int64" />
<Property Name="LastName" Type="Edm.String" />
<Property Name="FirstName" Type="Edm.String" />
<Property Name="Title" Type="Edm.String" />
<Property Name="ReportsTo" Type="Edm.Int64" />
<Property Name="BirthDate" Type="Edm.DateTimeOffset" />
<Property Name="HireDate" Type="Edm.DateTimeOffset" />
<Property Name="Address" Type="Edm.String" />
<Property Name="City" Type="Edm.String" />
<Property Name="State" Type="Edm.String" />
<Property Name="Country" Type="Edm.String" />
<Property Name="PostalCode" Type="Edm.String" />
<Property Name="Phone" Type="Edm.String" />
<Property Name="Fax" Type="Edm.String" />
<Property Name="Email" Type="Edm.String" />
</EntityType>
<EntityType Name="genres">
<Property Name="GenreId" Type="Edm.Int64" />
<Property Name="Name" Type="Edm.String" />
</EntityType>
<EntityType Name="invoice_items">
<Property Name="InvoiceLineId" Type="Edm.Int64" />
<Property Name="InvoiceId" Type="Edm.Int64" />
<Property Name="TrackId" Type="Edm.Int64" />
<Property Name="UnitPrice" Type="Edm.Decimal" />
<Property Name="Quantity" Type="Edm.Int64" />
</EntityType>
<EntityType Name="invoices">
<Property Name="InvoiceId" Type="Edm.Int64" />
<Property Name="CustomerId" Type="Edm.Int64" />
<Property Name="InvoiceDate" Type="Edm.DateTimeOffset" />
<Property Name="BillingAddress" Type="Edm.String" />
<Property Name="BillingCity" Type="Edm.String" />
<Property Name="BillingState" Type="Edm.String" />
<Property Name="BillingCountry" Type="Edm.String" />
<Property Name="BillingPostalCode" Type="Edm.String" />
<Property Name="Total" Type="Edm.Decimal" />
</EntityType>
<EntityType Name="media_types">
<Property Name="MediaTypeId" Type="Edm.Int64" />
<Property Name="Name" Type="Edm.String" />
</EntityType>
<EntityType Name="playlist_track">
<Property Name="PlaylistId" Type="Edm.Int64" />
<Property Name="TrackId" Type="Edm.Int64" />
</EntityType>
<EntityType Name="playlists">
<Property Name="PlaylistId" Type="Edm.Int64" />
<Property Name="Name" Type="Edm.String" />
</EntityType>
<EntityType Name="tracks">
<Property Name="TrackId" Type="Edm.Int64" />
<Property Name="Name" Type="Edm.String" />
<Property Name="AlbumId" Type="Edm.Int64" />
<Property Name="MediaTypeId" Type="Edm.Int64" />
<Property Name="GenreId" Type="Edm.Int64" />
<Property Name="Composer" Type="Edm.String" />
<Property Name="Milliseconds" Type="Edm.Int64" />
<Property Name="Bytes" Type="Edm.Int64" />
<Property Name="UnitPrice" Type="Edm.Decimal" />
</EntityType>
</Schema>
</edmx:DataServices>
</edmx:Edmx>'''

_test_table_row_count = 347

_test_table_json_length = 22373
_test_table_json_length_formatted = 43211


with open('pyproject.toml') as project_file:
    _test_project = toml.load(project_file)
    _test_version = _test_project['tool']['poetry']['version']


_test_sqlite_url = 'https://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip'
_test_sqlite_zip = zipfile.ZipFile(io.BytesIO(requests.get(_test_sqlite_url, timeout=10).content))
with _test_sqlite_zip.open('chinook.db') as zip_file:
    with open(_test_sqlite_filename, 'wb') as database_file:
        database_file.write(zip_file.read())


_odata_interface = sql_to_odata.ODataInterface(domain_name=_test_domain_name, sqlite_filename=_test_sqlite_filename)


def test_version():
    assert sql_to_odata.__version__ == _test_version


def test_get_table_names():
    table_names = _odata_interface.get_table_names()
    assert table_names == _test_table_names


def test_get_table_schema():
    table_schema = _odata_interface.get_table_schema(_test_table_name)
    assert table_schema == _test_table_schema


def test_get_table_schema_xml():
    table_schema_xml = _odata_interface.get_table_schema_xml(_test_table_name)
    assert table_schema_xml == _test_table_schema_xml


def test_get_database_schema():
    database_schema = _odata_interface.get_database_schema()
    assert database_schema == _test_database_schema


def test_get_database_schema_xml():
    database_schema_xml = _odata_interface.get_database_schema_xml()
    assert database_schema_xml == _test_database_schema_xml


def test_get_table_rows():
    table_rows = _odata_interface.get_table_rows(_test_table_name)
    assert len(table_rows) == _test_table_row_count


def test_get_table_json():
    table_json = _odata_interface.get_table_json(_test_table_name)
    assert len(table_json) == _test_table_json_length
    table_json = _odata_interface.get_table_json(_test_table_name, formatted=True)
    assert len(table_json) == _test_table_json_length_formatted


def test_datatype_mappings():
    datatype_to_odata = sql_to_odata.ODataInterface.datatype_to_odata
    assert datatype_to_odata('INTEGER') == 'Edm.Int64'
    assert datatype_to_odata('REAL') == 'Edm.Double'
    assert datatype_to_odata('NUMERIC(10,4)') == 'Edm.Decimal'
    assert datatype_to_odata('TEXT') == 'Edm.String'
    assert datatype_to_odata('NVARCHAR(42)') == 'Edm.String'
    assert datatype_to_odata('BLOB') == 'Edm.Binary'
    assert datatype_to_odata('DATETIME') == 'Edm.DateTimeOffset'
    with pytest.raises(ValueError) as error:
        assert datatype_to_odata('does-not-exist')
    assert error.value.args[0] == 'Unknown data type: does-not-exist'


def test_missing_database_file():
    with pytest.raises(sqlite3.OperationalError) as error:
        odata_interface = sql_to_odata.ODataInterface(domain_name=_test_domain_name, sqlite_filename='does-not-exist')
        odata_interface.get_table_names()
    assert error.value.args[0] == 'unable to open database file'


def test_bad_database_file():
    with pytest.raises(sqlite3.DatabaseError) as error:
        odata_interface = sql_to_odata.ODataInterface(domain_name=_test_domain_name, sqlite_filename='README.md')
        odata_interface.get_table_names()
    assert error.value.args[0] == 'file is not a database'


def test_invalid_table_name():
    with pytest.raises(ValueError) as error:
        _odata_interface.get_table_rows('does-not-exist')
    assert error.value.args[0] == 'Table not found: does-not-exist'
