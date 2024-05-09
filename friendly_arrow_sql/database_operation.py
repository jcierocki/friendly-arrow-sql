import pyarrow as pa
import adbc_driver_manager.dbapi as adbc

from abc import ABC, abstractmethod


class DatabaseOperationError(Exception):
    pass


class AbstractDatabaseOperation(ABC):
    @abstractmethod
    def execute_with_cursor(self, cursor: adbc.Cursor) -> pa.Table | None:
        pass


class SelectOperation(AbstractDatabaseOperation):
    query: str

    def __init__(self, query: str):
        self.query = query

    def execute_with_cursor(self, cursor: adbc.Cursor) -> pa.Table:
        try:
            cursor.execute(self.query)
        except Exception as e:
            raise DatabaseOperationError("Failed to execute query: \n" + self.query) from e

        return cursor.fetch_arrow_table()


class BulkInsertOperation(AbstractDatabaseOperation):
    table_name: str
    data: pa.Table
    schema: str

    def __init__(self, table_name: str, data: pa.Table, schema: str | None = None):
        self.table_name = table_name
        self.data = data
        self.schema = schema

    def execute_with_cursor(self, cursor: adbc.Cursor) -> None:
        try:
            cursor.adbc_ingest(self.table_name, self.data, db_schema_name=self.schema, mode="append")
        except Exception as e:
            schema_prefix = '' if self.schema is None else f"{self.schema}."
            raise DatabaseOperationError(f"Failed to bulk insert data into {schema_prefix}{self.table_name}") from e





