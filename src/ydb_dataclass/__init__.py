"""
YDB Dataclass - библиотека для работы с YDB через dataclass
"""

from .decorator import ydb_dataclass, YDBFieldInfo
from .types import YDB
from .queries import (
    create_table_query,
    insert_query,
    select_query,
    update_query,
    upsert_query,
    delete_query,
    generate_where_clause
)

__version__ = "0.1.0"
__all__ = [
    "ydb_dataclass",
    "YDB",
    "YDBFieldInfo",
    "create_table_query",
    "insert_query",
    "select_query",
    "update_query",
    "upsert_query",
    "delete_query",
    "generate_where_clause",
]