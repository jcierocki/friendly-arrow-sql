import pyarrow as pa
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
                 schema: str | None = None,
                 mode: Literal['append', 'create', 'create_append', 'replace'] = "append"):
        self.table_name = table_name
        self.data = data
        self.schema = schema
        self.mode = mode

    def execute_with_cursor(self, cursor: adbc.Cursor) -> None:
        try:
            cursor.adbc_ingest(self.table_name, self.data, db_schema_name=self.schema, mode=self.mode)
        except Exception as e:
            schema_prefix = '' if self.schema is None else f"{self.schema}."
            raise DatabaseOperationError(f"Failed to bulk insert data into {schema_prefix}{self.table_name}") from e





