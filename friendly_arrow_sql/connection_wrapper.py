import pyarrow as pa
import adbc_driver_manager.dbapi as adbc
import sys

from urllib.parse import urlparse, ParseResult
from typing import Literal, TypeAlias
from types import ModuleType

import database_operation as ops

from utils import package_load_validate


DbOperationError: TypeAlias = ops.DatabaseOperationError
DbOperation: TypeAlias = ops.AbstractDatabaseOperation
StateModifyingDbOperation: TypeAlias = ops.AbstractStateModifyingDatabaseOperation


class ArgumentError(ValueError):
    pass


class DatabaseConnection:
    __uri: ParseResult
    __driver: ModuleType
    __conn: adbc.Connection | None

    def get_uri(self, hide_password: bool = True) -> str:
        uri_raw = self.__uri.geturl()

        if not hide_password:
            return uri_raw

        # TODO rethink, this will fail for SQLite
        return uri_raw.replace(f":{self.__uri.password}@", ":***@")

    def __init__(self, uri: str, reusable: bool = True):
        self.__uri = urlparse(uri)
        sql_dialect_name = self.__uri.scheme
        adbc_driver_name = f"adbc_driver_{sql_dialect_name}.dbapi"

        package_load_validate(adbc_driver_name, "adbc_driver")
        self.__driver = sys.modules["adbc_driver"]

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

    @classmethod
    def _validate_only_state_modifying_operations(cls, operations: list[DbOperation]) -> None:
        if any(not isinstance(op_, StateModifyingDbOperation) for op_ in operations):
            raise ArgumentError(
                "Multiple operations transactions are supported only for StateModifyingDatabaseOperation")

    def execute(self, operations: DbOperation | list[DbOperation]) -> pa.Table | None:
        single_output = False
        if isinstance(operations, list):
            # TODO rethink whether such a validation is needed
            # there's no point to chain multiple selects within a single transaction
            self._validate_only_state_modifying_operations(operations)
        else:
            single_output = True
            operations = [operations]

        conn_ = self._get_connection()
        query_results: list[pa.Table | None] = []
        with conn_.cursor() as cur:
            for op_ in operations:
                try:
                    out = op_.execute_with_cursor(cur)
                except DbOperationError as e:
                    conn_.rollback()
                    raise e

                query_results.append(out)

        conn_.commit()
        if self.__conn is None:
            conn_.close()

        if single_output:
            # otherwise returning None by default
            return query_results[0]
