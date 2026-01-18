# YDB Dataclass

Библиотека для удобной работы с YDB через dataclass с явными типами YDB.

## Установка

### Установка из репозитория GitHub:

```bash
pip install git+https://github.com/Korean-DOG/ydb-dataclass.git
```

**Быстрый старт**
1. Определение моделей
```python
from typing import Optional
from ydb_dataclass import ydb_dataclass, YDB

@ydb_dataclass
class User:
    id: YDB.int64
    username: YDB.utf8
    email: Optional[YDB.utf8]
    age: Optional[YDB.uint64]
    is_active: YDB.bool = True
    created_at: YDB.timestamp
    metadata: YDB.json = "{}"

@ydb_dataclass
class Order:
    id: YDB.int64
    user_id: YDB.int64
    amount: YDB.decimal(10, 2)  # Decimal с точностью 10, масштабом 2
    status: YDB.utf8
    items: YDB.json
```
2. Работа с объектами
```python
# Создание объекта
import time

user = User(
    id=1,
    username="john_doe",
    email="john@example.com",
    age=25,
    is_active=True,
    created_at=int(time.time() * 1_000_000),  # микросекунды
    metadata='{"theme": "dark", "notifications": true}'
)

# Работает как обычный dataclass
print(user)  # User(id=1, username='john_doe', email='john@example.com', ...)
print(user.username)  # john_doe

# Получение схемы YDB
schema = User.get_ydb_schema()
print(schema)
# {
#   'id': 'Int64',
#   'username': 'Utf8',
#   'email': 'Optional<Utf8>',
#   'age': 'Optional<Uint64>',
#   'is_active': 'Bool',
#   'created_at': 'Timestamp',
#   'metadata': 'Json'
# }
```
3. Конвертация для YDB
```python
# Конвертация в YDB словарь
ydb_dict = user.to_ydb_dict()
print(ydb_dict)
# {
#   'id': <ydb.Int64(1)>,
#   'username': <ydb.Utf8('john_doe')>,
#   'email': <ydb.Utf8('john@example.com')>,
#   'age': <ydb.Uint64(25)>,
#   'is_active': <ydb.Bool(True)>,
#   'created_at': <ydb.Timestamp(...)>,
#   'metadata': <ydb.Json('{"theme": "dark", "notifications": true}')>
# }

# Создание из результата запроса YDB
# Предположим, что row - результат запроса из YDB
# user_from_db = User.from_ydb_row(row)

# Создание из обычного словаря
user_data = {
    "id": 2,
    "username": "jane_doe",
    "email": "jane@example.com",
    "age": 30,
    "is_active": True,
    "created_at": int(time.time() * 1_000_000),
    "metadata": '{"theme": "light"}'
}
user2 = User.from_dict(user_data)
```
4. Генерация SQL запросов
```python
from ydb_dataclass.queries import (
    create_table_query,
    insert_query,
    select_query,
    update_query,
    delete_query,
    upsert_query,
    generate_where_clause,
    prepare_params
)

# Создание таблицы
create_sql = create_table_query(
    "users",
    User._ydb_fields,
    primary_key=["id"],
    partition_by=["id"]
)
print(create_sql)
# CREATE TABLE `users` (
#   id Int64,
#   username Utf8,
#   email Optional<Utf8>,
#   age Optional<Uint64>,
#   is_active Bool,
#   created_at Timestamp,
#   metadata Json,
#   PRIMARY KEY (id),
#   PARTITION BY (id)
# )

# INSERT запрос
insert_sql = insert_query("users", User._ydb_fields)
print(insert_sql)
# INSERT INTO `users` (id, username, email, age, is_active, created_at, metadata) VALUES ($id, $username, $email, $age, $is_active, $created_at, $metadata)

# UPSERT запрос (INSERT OR UPDATE)
upsert_sql = upsert_query("users", User._ydb_fields)
print(upsert_sql)
# UPSERT INTO `users` (id, username, email, age, is_active, created_at, metadata) VALUES ($id, $username, $email, $age, $is_active, $created_at, $metadata)

# SELECT с фильтрацией
select_sql = select_query(
    "users",
    User._ydb_fields,
    where={"is_active": True, "age": 25},
    order_by=["created_at DESC"],
    limit=10
)
print(select_sql)
# SELECT id, username, email, age, is_active, created_at, metadata FROM `users` WHERE is_active = $is_active AND age = $age ORDER BY created_at DESC LIMIT 10

# UPDATE запрос
update_sql = update_query(
    "users",
    User._ydb_fields,
    where={"id": 1},
    update_fields=["email", "is_active"]
)
print(update_sql)
# UPDATE `users` SET email = $email, is_active = $is_active WHERE id = $id

# DELETE запрос
delete_sql = delete_query(
    "users",
    where={"id": 1}
)
print(delete_sql)
# DELETE FROM `users` WHERE id = $id
```
5. Полный пример работы с YDB
```python
import ydb
import asyncio
from typing import List

# Подключение к YDB
async def create_driver():
    endpoint = "grpcs://ydb.serverless.yandexcloud.net:2135"
    database = "/ru-central1/b1g..."
    credentials = ydb.iam.ServiceAccountCredentials.from_file("sa-key.json")
    
    driver_config = ydb.DriverConfig(
        endpoint, 
        database, 
        credentials=credentials
    )
    
    driver = ydb.aio.Driver(driver_config)
    await driver.wait(timeout=5)
    return driver

# CRUD операции
async def crud_example():
    driver = await create_driver()
    session = await driver.table_client.session().create()
    
    # Создание таблицы
    create_table_sql = create_table_query(
        "users",
        User._ydb_fields,
        primary_key=["id"]
    )
    
    await session.execute_scheme(create_table_sql)
    
    # Вставка данных
    user = User(
        id=1,
        username="test_user",
        email="test@example.com",
        age=25,
        is_active=True,
        created_at=int(time.time() * 1_000_000),
        metadata='{}'
    )
    
    insert_sql = insert_query("users", User._ydb_fields)
    prepared_query = await session.prepare(insert_sql)
    
    await session.transaction().execute(
        prepared_query,
        user.to_ydb_dict(),
        commit_tx=True
    )
    
    # Выборка данных
    select_sql = select_query(
        "users",
        User._ydb_fields,
        where={"is_active": True}
    )
    
    prepared_select = await session.prepare(select_sql)
    result = await session.transaction().execute(
        prepared_select,
        {"is_active": True},
        commit_tx=True
    )
    
    # Конвертация результатов
    users: List[User] = []
    for row in result[0].rows:
        user = User.from_ydb_row(row)
        users.append(user)
    
    await session.close()
    await driver.stop()
    
    return users

# Запуск примера
if __name__ == "__main__":
    result = asyncio.run(crud_example())
    for user in result:
        print(f"User: {user.username}, Age: {user.age}")
```
6. Работа с параметризованными запросами
```python
# Подготовка параметров для WHERE IN
conditions = {"id": [1, 2, 3], "status": "active"}
where_clause = generate_where_clause(conditions)
print(where_clause)
# id IN ($id_0, $id_1, $id_2) AND status = $status

params = prepare_params({}, conditions)
print(params)
# {
#   'id_0': 1,
#   'id_1': 2,
#   'id_2': 3,
#   'status': 'active'
# }

# Использование в запросе
select_sql = f"SELECT * FROM users WHERE {where_clause}"
print(select_sql)
# SELECT * FROM users WHERE id IN ($id_0, $id_1, $id_2) AND status = $status
```
7. Индексы и партиционирование
```python
# Создание таблицы с индексами
create_sql_with_indexes = create_table_query(
    "orders",
    Order._ydb_fields,
    primary_key=["id"],
    partition_by=["user_id"],
    indexes={
        "idx_user_status": ["user_id", "status"],
        "idx_status": ["status"]
    }
)

print(create_sql_with_indexes)
# CREATE TABLE `orders` (
#   id Int64,
#   user_id Int64,
#   amount Decimal(10, 2),
#   status Utf8,
#   items Json,
#   PRIMARY KEY (id),
#   PARTITION BY (user_id),
#   INDEX idx_user_status GLOBAL ON (user_id, status),
#   INDEX idx_status GLOBAL ON (status)
# )
```
8. Пример для Yandex Cloud Functions
```python
import ydb
import json
import os
from typing import Dict, Any, List

@ydb_dataclass
class LogEntry:
    id: YDB.int64
    function_name: YDB.utf8
    timestamp: YDB.timestamp
    level: YDB.utf8
    message: YDB.utf8
    context: YDB.json = "{}"

async def ycf_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Обработчик Yandex Cloud Function
    """
    # Получаем параметры подключения из переменных окружения
    endpoint = os.getenv("YDB_ENDPOINT")
    database = os.getenv("YDB_DATABASE")
    
    # Создаем драйвер YDB
    driver = ydb.aio.Driver(
        endpoint=endpoint,
        database=database,
        credentials=ydb.iam.MetadataUrlCredentials(),
    )
    
    try:
        await driver.wait(timeout=5)
        session = await driver.table_client.session().create()
        
        # Создаем объект лога
        log_entry = LogEntry(
            id=int(context.request_id),
            function_name=context.function_name,
            timestamp=int(time.time() * 1_000_000),
            level="INFO",
            message=json.dumps(event),
            context=json.dumps({
                "function_version": context.function_version,
                "memory_limit": context.memory_limit,
            })
        )
        
        # Вставляем лог в таблицу
        insert_sql = insert_query("function_logs", LogEntry._ydb_fields)
        prepared = await session.prepare(insert_sql)
        
        await session.transaction().execute(
            prepared,
            log_entry.to_ydb_dict(),
            commit_tx=True
        )
        
        # Получаем последние 10 логов
        select_sql = select_query(
            "function_logs",
            LogEntry._ydb_fields,
            where={"function_name": context.function_name},
            order_by=["timestamp DESC"],
            limit=10
        )
        
        prepared_select = await session.prepare(select_sql)
        result = await session.transaction().execute(
            prepared_select,
            {"function_name": context.function_name},
            commit_tx=True
        )
        
        # Конвертируем результаты
        logs: List[Dict[str, Any]] = []
        for row in result[0].rows:
            entry = LogEntry.from_ydb_row(row)
            logs.append({
                "id": entry.id,
                "timestamp": entry.timestamp,
                "level": entry.level,
                "message": entry.message
            })
        
        await session.close()
        
        return {
            "statusCode": 200,
            "body": {
                "logged": True,
                "recent_logs": logs
            }
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": {
                "error": str(e)
            }
        }
    finally:
        await driver.stop()
```