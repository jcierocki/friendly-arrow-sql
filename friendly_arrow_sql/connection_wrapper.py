import pyarrow as pa
import adbc_driver_manager.dbapi as adbc
import os
import sys

from urllib.parse import urlparse, ParseResult
from typing import Literal
from types import ModuleType

import database_operation as ops

from utils import package_load_validate


class MissingSchemaError(Exception):
    pass


class SettingSchemaError(NotImplementedError):
    pass


class DatabaseConnection:
    __uri: ParseResult
    __driver: ModuleType
    __conn: adbc.Connection | None
    schema: str | None

    def get_uri(self, hide_password: bool = True) -> str:
        uri_raw = self.__uri.geturl()

        if not hide_password:
            return uri_raw

        return uri_raw.replace(f":{self.__uri.password}@", ":***@")

    def _validate_schema(self, schema: str):
        with self.__driver.connect(self.__uri.geturl()) as conn:
            if schema not in conn.adbc_get_objects(depth="db_schemas"):
                raise MissingSchemaError(f"Database schema '{schema}' does not exist")

    def __init__(self, uri: str, reusable: bool = True, schema: str | None = None):
        self.__uri = urlparse(uri)
        sql_dialect_name = self.__uri.scheme
        adbc_driver_name = f"adbc_driver_{sql_dialect_name}.dbapi"

        package_load_validate(adbc_driver_name, "adbc_driver")
        self.__driver = sys.modules["adbc_driver"]

        if schema is not None:  # makes no impact yet
            if sql_dialect_name != "postgresql":
                SettingSchemaError("Setting a default schema for the whole transaction " +
                                   "is currently supported only for PostgreSQL")

            self._validate_schema(schema)
            self.schema = schema

        if reusable:
            self.__conn = self.__driver.connect(uri, autocommit=False)
        else:
            self.__conn = None

    def __del__(self):
        if self.__conn is not None:
            self.__conn.close()

    def __repr__(self):
        return f"DatabaseConnection({self.get_uri()})"

    def _get_connection(self) -> adbc.Connection:
        if self.__conn is None:
            return self.__driver.connect(self.get_uri(hide_password=False), autocommit=False)
        return self.__conn

    def execute(self,
                operations: ops.AbstractDatabaseOperation | list[ops.AbstractDatabaseOperation]
                ) -> list[pa.Table | None]:
        if not isinstance(operations, list):
            operations = [operations]

        conn_ = self._get_connection()

        query_results: list[pa.Table | None] = []
        with conn_.cursor() as cur:
            for op_ in operations:
                try:
                    out = op_.execute_with_cursor(cur)
                except ops.DatabaseOperationError as e:
                    conn_.rollback()
                    raise e

                query_results.append(out)

        conn_.commit()
        if self.__conn is None:
            conn_.close()

        return query_results

