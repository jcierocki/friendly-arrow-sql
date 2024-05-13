import warnings
import re

import pyarrow as pa
import numpy as np
import adbc_driver_manager.dbapi as adbc

from abc import ABC, abstractmethod
from typing import Literal


class DatabaseOperationError(Exception):
    pass


class AbstractDatabaseOperation(ABC):
    @abstractmethod
    def execute_with_cursor(self, cursor: adbc.Cursor) -> pa.Table | None:
        pass


class AbstractStateModifyingDatabaseOperation(AbstractDatabaseOperation):
    @abstractmethod
    def execute_with_cursor(self, cursor: adbc.Cursor) -> None:
        pass


class QueryOnlyOperation(AbstractDatabaseOperation):
    query: str

    def __init__(self, query: str):
        self.query = query

    def execute_with_cursor(self, cursor: adbc.Cursor):
        try:
            cursor.execute(self.query)
        except Exception as e:
            raise DatabaseOperationError("Failed to execute query: \n" + self.query) from e


class SelectOperation(QueryOnlyOperation):
    def execute_with_cursor(self, cursor: adbc.Cursor) -> pa.Table:
        super().execute_with_cursor(cursor)

        return cursor.fetch_arrow_table()


class BulkInsertOperation(AbstractStateModifyingDatabaseOperation):
    table_name: str
    data: pa.Table
    schema: str
    mode: Literal['append', 'create', 'create_append', 'replace']

    def __init__(self,
                 table_name: str,
                 data: pa.Table,
                 schema: str | None = None,  # has no effect for SQLite
                 mode: Literal['append', 'create', 'create_append', 'replace'] = "append"):

        self.table_name = table_name
        self.data = data
        self.schema = schema
        self.mode = mode

    def _reset_schema_sqlite(self, cursor: adbc.Cursor):
        # this method resets the self.schema if detects that the 'cursor' was created using SQLite connection
        # (SQLite does not support schemas)
        if cursor._conn.__class__.__name__ == "AdbcSqliteConnection":
            self.schema = None
            warnings.warn("Custom schemas are not supported by SQLite")

    def execute_with_cursor(self, cursor: adbc.Cursor) -> None:
        self._reset_schema_sqlite(cursor)

        try:
            cursor.adbc_ingest(self.table_name,
                               self.data,
                               db_schema_name=self.schema,
                               mode=self.mode)
        except Exception as e:
            schema_prefix = '' if self.schema is None else f"{self.schema}."
            raise DatabaseOperationError(f"Failed to bulk insert data into {schema_prefix}{self.table_name}") from e


class UpdateDeleteOperation(AbstractStateModifyingDatabaseOperation):
    query: str
    data: pa.Table
    auto_adjust_dialect: bool

    def _adjust_query_sql_dialect(self, cursor: adbc.Cursor):
        # this method adjusts the query and data column order if detects that the 'cursor' was created using SQLite
        # connection (SQLite requires different placeholder types as well as the data to be initially sorted)
        if cursor._conn.__class__.__name__ == "AdbcSqliteConnection":
            self.schema = None

            field_name_placeholders = re.findall(r"\$\d", self.query)
            column_order = np.argsort(field_name_placeholders)

            self.query = re.sub(r"\$\d", '?', self.query)
            self.data = self.data.select(np.array(self.data.column_names)[column_order])

    def __init__(self,
                 query: str,
                 data: pa.Table,
                 auto_adjust_dialect: bool = True
                 # set ^^^ to False if you use Postgres or you manually adjusted the query and the data to
                 # fit SQLite syntax
                 ):
        self.query = query
        self.data = data
        self.auto_adjust_dialect = auto_adjust_dialect

    def execute_with_cursor(self, cursor: adbc.Cursor) -> None:
        if self.auto_adjust_dialect:
            self._adjust_query_sql_dialect(cursor)

        try:
            cursor.executemany(self.query, self.data)
        except Exception as e:
            raise DatabaseOperationError("Failed to execute query: \n" + self.query) from e

