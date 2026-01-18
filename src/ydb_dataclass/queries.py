"""
Генерация SQL запросов для YDB dataclass
"""

from typing import Dict, Any, List, Optional, Union
from .decorator import YDBFieldInfo


def create_table_query(
        table_name: str,
        ydb_fields: Dict[str, YDBFieldInfo],
        primary_key: Optional[List[str]] = None,
        partition_by: Optional[List[str]] = None,
        indexes: Optional[Dict[str, List[str]]] = None
) -> str:
    """
    Создать SQL запрос для создания таблицы

    Args:
        table_name: Имя таблицы
        ydb_fields: Словарь полей YDB
        primary_key: Список полей первичного ключа
        partition_by: Список полей для партиционирования
        indexes: Словарь индексов {имя_индекса: [поля]}

    Returns:
        SQL запрос
    """
    # Генерация колонок
    columns = []
    for name, field in ydb_fields.items():
        columns.append(f"  {name} {field.ydb_type.ydb_type}")

    # Партиции (Shards)
    partition_clause = ""
    if partition_by:
        partition_fields = ", ".join(partition_by)
        partition_clause = f"\n  PARTITION BY ({partition_fields})"

    # Первичный ключ
    pk_clause = ""
    if primary_key:
        pk_fields = ", ".join(primary_key)
        pk_clause = f"\n  PRIMARY KEY ({pk_fields})"

    # Индексы
    indexes_clause = ""
    if indexes:
        index_definitions = []
        for index_name, index_fields in indexes.items():
            fields_str = ", ".join(index_fields)
            index_definitions.append(f"  INDEX {index_name} GLOBAL ON ({fields_str})")
        indexes_clause = "\n" + "\n".join(index_definitions)

    # Собираем запрос
    columns_str = ",\n".join(columns)
    query = f"CREATE TABLE `{table_name}` (\n{columns_str}"

    if pk_clause:
        query += f",{pk_clause}"

    if partition_clause:
        query += f",{partition_clause}"

    if indexes_clause:
        query += f",{indexes_clause}"

    query += "\n)"

    return query


def insert_query(
        table_name: str,
        ydb_fields: Dict[str, YDBFieldInfo],
        on_conflict: Optional[str] = None
) -> str:
    """
    Создать SQL запрос для INSERT

    Args:
        table_name: Имя таблицы
        ydb_fields: Словарь полей YDB
        on_conflict: Действие при конфликте (REPLACE, UPDATE, NOTHING)

    Returns:
        SQL запрос
    """
    columns = list(ydb_fields.keys())
    placeholders = [f"${col}" for col in columns]

    columns_str = ", ".join(columns)
    placeholders_str = ", ".join(placeholders)

    query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders_str})"

    if on_conflict == "REPLACE":
        query = query.replace("INSERT", "REPLACE")
    elif on_conflict == "UPDATE":
        # Для YDB используем UPSERT
        query = f"UPSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders_str})"

    return query


def upsert_query(
        table_name: str,
        ydb_fields: Dict[str, YDBFieldInfo]
) -> str:
    """
    Создать SQL запрос для UPSERT

    Args:
        table_name: Имя таблицы
        ydb_fields: Словарь полей YDB

    Returns:
        SQL запрос
    """
    columns = list(ydb_fields.keys())
    placeholders = [f"${col}" for col in columns]

    columns_str = ", ".join(columns)
    placeholders_str = ", ".join(placeholders)

    return f"UPSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders_str})"


def select_query(
        table_name: str,
        ydb_fields: Optional[Dict[str, YDBFieldInfo]] = None,
        where: Optional[Dict[str, Any]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        columns: Optional[List[str]] = None
) -> str:
    """
    Создать SQL запрос для SELECT

    Args:
        table_name: Имя таблицы
        ydb_fields: Словарь полей YDB (для проверки)
        where: Условия WHERE
        order_by: Сортировка
        limit: Лимит записей
        offset: Смещение
        columns: Список колонок для выборки

    Returns:
        SQL запрос
    """
    # Выбор колонок
    if columns:
        columns_str = ", ".join(columns)
    elif ydb_fields:
        columns_str = ", ".join(ydb_fields.keys())
    else:
        columns_str = "*"

    query = f"SELECT {columns_str} FROM `{table_name}`"

    # Условия WHERE
    if where:
        where_clause = generate_where_clause(where)
        query += f" WHERE {where_clause}"

    # Сортировка
    if order_by:
        order_str = ", ".join(order_by)
        query += f" ORDER BY {order_str}"

    # Лимит и смещение
    if limit is not None:
        query += f" LIMIT {limit}"
        if offset is not None:
            query += f" OFFSET {offset}"

    return query


def update_query(
        table_name: str,
        ydb_fields: Dict[str, YDBFieldInfo],
        where: Optional[Dict[str, Any]] = None,
        update_fields: Optional[List[str]] = None
) -> str:
    """
    Создать SQL запрос для UPDATE

    Args:
        table_name: Имя таблицы
        ydb_fields: Словарь полей YDB
        where: Условия WHERE
        update_fields: Поля для обновления (если None - все кроме where)

    Returns:
        SQL запрос
    """
    # Определяем поля для обновления
    all_fields = list(ydb_fields.keys())

    if update_fields:
        set_fields = [f for f in update_fields if f in all_fields]
    else:
        # По умолчанию обновляем все поля
        set_fields = all_fields

    # Формируем SET часть
    set_clauses = [f"{field} = ${field}" for field in set_fields]
    set_clause = ", ".join(set_clauses)

    query = f"UPDATE `{table_name}` SET {set_clause}"

    # Условия WHERE
    if where:
        where_clause = generate_where_clause(where)
        query += f" WHERE {where_clause}"

    return query


def delete_query(
        table_name: str,
        where: Dict[str, Any]
) -> str:
    """
    Создать SQL запрос для DELETE

    Args:
        table_name: Имя таблицы
        where: Условия WHERE

    Returns:
        SQL запрос
    """
    query = f"DELETE FROM `{table_name}`"

    if where:
        where_clause = generate_where_clause(where)
        query += f" WHERE {where_clause}"

    return query


def generate_where_clause(conditions: Dict[str, Any]) -> str:
    """
    Сгенерировать WHERE clause из словаря условий

    Args:
        conditions: Словарь условий {поле: значение}

    Returns:
        WHERE clause
    """
    clauses = []

    for field, value in conditions.items():
        if value is None:
            clauses.append(f"{field} IS NULL")
        elif isinstance(value, list) or isinstance(value, tuple):
            # IN clause
            placeholders = ", ".join([f"${field}_{i}" for i in range(len(value))])
            clauses.append(f"{field} IN ({placeholders})")
        else:
            clauses.append(f"{field} = ${field}")

    return " AND ".join(clauses)


def prepare_params(
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        where: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Подготовить параметры для запроса

    Args:
        data: Данные для вставки/обновления
        where: Условия WHERE

    Returns:
        Словарь параметров
    """
    params = {}

    if isinstance(data, dict):
        # Одиночный словарь
        for key, value in data.items():
            params[key] = value
    elif isinstance(data, list):
        # Список словарей
        for i, item in enumerate(data):
            for key, value in item.items():
                params[f"{key}_{i}"] = value

    # Добавляем условия WHERE
    if where:
        for key, value in where.items():
            if isinstance(value, list) or isinstance(value, tuple):
                for i, v in enumerate(value):
                    params[f"{key}_{i}"] = v
            else:
                params[key] = value

    return params