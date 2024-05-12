# friendly-arrow-sql

## Description

**friendly-arrow-sql** is a high-level, low-boilerplate database connectivity interface, build on top of [_ADBC_](https://arrow.apache.org/adbc/current/index.html) and using [_pyarrow_](https://arrow.apache.org/docs/python/index.html) for IO. It primarily intended for data (ETL) workflows data that:
- (preferably) collect data from **SQL** databases
- benefits from low latency
- end up writing/updating multiple DB tables, without the risk of data corruption

The current version supports **PostgreSQL**, **SQLite** and **Arrow Flight SQL** (not tested) database engines. **Snowflake** support will be added, as this engine is already supported by _ADBC_. 

## Why _friendly-arrow-sql_?

While I was working for my last two companies, the large part of my job was to develop data pipelines in [_polars_](https://github.com/pola-rs/polars) (I highly recommend it <3) or _pandas_ (its terrible, but sometimes management forces you to use it). Those pipelines were at the end required to upload all the resulting data to **PostgreSQL** database. Both _polars_ and _pandas_ provide some API for database connectivity but those have some significant issues:

- lack of support for "transactions"
- additional latency introduced by opening the whole connection every time you execute the query
- often poor performance, especially for `pandas.DataFrame().to_sql()`

At the same time using [_psycopg_](https://www.psycopg.org), the default _PostgreSQL_ _Python_ connector, has its own drawbacks:
- requires the developer to implement the connection and transaction management boilerplate code from scratch
- the I/O relies solely on _Python_ native list of dicts or tuples, requiring additional conversion to DataFrame format which generates extra latency

Popular ORMs inherit the 2nd issue, while also introduce another latency due to parsing. They also require you to map every database table which sometimes is not desired when you develop data engineering solution rather than web backend. 

Finally, I discovered [_ADBC_](https://arrow.apache.org/adbc/current/index.html) and its _Python_ API, promising to solve my issues. Here there are some real pros of using _ADBC_:
- performance is great (I will upload some benchmarks later)
- methods both take and return `pyarrow.Table` objects that can be converted in a zero-copy manner to `polars.DataFrame` both ways
- _psycopg_ alike transaction management

Although the _adbc-driver-manager_ still suffers from the same boilerplate issues as _psycopg_, while also is in rather early stage of development. That results for example in only very brief documentation, missing many usage examples. Along with the boilerplate that requires a little bit more understanding of low-level database mechanisms, that makes this tool not very user-friendly. Finally, it requires separate driver packages for every SQL engine to work with which requires additional management.

To solve all the issues mentioned above I started writing some rather bare-bones wrapper for companies I worked for, to at the end, make a try to turn the know-how I gained into nice package. 



## Installation
The package is currently uploaded to **Test PyPI**. You can install it from there using the _pip_ `--index-url` flag.
 ```shell
pip install --index-url https://test.pypi.org/simple/ friendly-arrow-sql
# OR USING uv (recommended)
uv pip install --index-url https://test.pypi.org/simple/ friendly-arrow-sql
```

The package by default supports only **PostgreSQL** connections and if you want to use this tool with different DB engines (and so install different ADBC driver packages) you need to utilise the installation options:
- `friendly-arrow-sql[sqlite]` - additionally supports **SQLite**
- `friendly-arrow-sql[flightsql]` - additionally supports **Arrow Flight SQL**
- `friendly-arrow-sql[all]` - supports all available database engines (mentioned above)

For example:
```shell
pip install --index-url https://test.pypi.org/simple/ friendly-arrow-sql[all]
```


## Usage

Here I put some simple examples how you can use my tool:

```python
import pyarrow as pa
import friendly_arrow_sql as asql

df_example1: pa.Table = ...
df_example2: pa.Table = ...

conn = asql.DatabaseConnection("postgresql://login:password&@localhost:5432/database_name")

# Imagine that 'example_table2' has an FK on 'example_table1'.
# In that case insert order matters, and you typically want the whole transaction to fail 
# and be rolled back in case of error.
conn.execute([
    asql.BulkInsertOperation("example_table1", df_example1),
    asql.BulkInsertOperation("example_table2", df_example2)
])

df_example2_out: pa.Table = conn.execute([
    asql.SelectOperation("SELECT * FROM example_table2")
])

```

## TODO
1. support for `UPDATE` and `DELETE` operations
2. API Reference documentation
3. tests (using **SQLite**)
4. consider using `adbc_driver_manager.AdbcDatabase(driver=...)` instead of manually importing driver packages to `sys.modules`
5. more usage examples
6. move all database operation wrapper to the separate subpackage
7. Snowflake support
8. upload package to the main **PyPI**
9. add support for [_connectorx_](https://github.com/sfu-db/connector-x) backend
10. add support for [_duckdb_](https://github.com/duckdb/duckdb) backend

## Contributing
This is my first real open source Python package thus I will be grateful for any feedback regarding:
- tool functionality
- API design
- implementation

Feel free to add issues, not only when you face bugs but also with feedback mentioned above. I will try to address them ASAP.

Finally, feel free to fork the repo and submit pull requests with improvements.


## License
This project is licensed under the terms of the MIT license.

## Project status
This project is currently in the early stage of development.