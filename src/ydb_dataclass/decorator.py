"""
Декоратор @ydb_dataclass для создания YDB-совместимых dataclass
"""

from dataclasses import dataclass, fields
from typing import Dict, Any, Type, get_type_hints, get_origin, get_args, ClassVar
import ydb

from .types import YDB


class YDBFieldInfo:
    """Информация о поле YDB dataclass"""

    def __init__(self, name: str, ydb_type, default=...):
        self.name = name
        self.ydb_type = ydb_type
        self.default = default
        self.python_type = self._get_python_type(ydb_type)

    def _get_python_type(self, ydb_type):
        """Получить Python тип из YDB типа"""
        if isinstance(ydb_type, YDB.Optional):
            return self._get_python_type(ydb_type.inner_type)
        elif hasattr(ydb_type, 'python_type'):
            return ydb_type.python_type
        return Any

    def get_ydb_value(self, value):
        """Получить YDB значение из Python значения"""
        if value is None:
            return ydb.Null()

        if isinstance(self.ydb_type, YDB.Optional):
            if value is None:
                return ydb.Null()
            return self.ydb_type.ydb_value(value)

        if hasattr(self.ydb_type, 'ydb_value'):
            return self.ydb_type.ydb_value(value)

        # По умолчанию в строку
        return ydb.Utf8(str(value))


def _is_ydb_type(t):
    """Проверка, является ли тип YDB типом"""
    # Проверяем базовые YDB типы
    for attr_name in dir(YDB):
        attr = getattr(YDB, attr_name)
        if isinstance(attr, type) and hasattr(attr, 'python_type'):
            if t is attr:
                return True

    # Проверяем Optional[YDB тип]
    origin = get_origin(t)
    if origin is not None:
        args = get_args(t)
        if len(args) == 2 and args[1] == type(None):
            inner_type = args[0]
            for attr_name in dir(YDB):
                attr = getattr(YDB, attr_name)
                if isinstance(attr, type) and hasattr(attr, 'python_type'):
                    if inner_type is attr:
                        return True

    return False


def _extract_ydb_type(t):
    """Извлечь YDB тип из аннотации"""
    # Базовый YDB тип
    for attr_name in dir(YDB):
        attr = getattr(YDB, attr_name)
        if isinstance(attr, type) and hasattr(attr, 'python_type'):
            if t is attr:
                return attr

    # Optional[YDB тип]
    origin = get_origin(t)
    if origin is not None:
        args = get_args(t)
        if len(args) == 2 and args[1] == type(None):
            inner_type = args[0]
            for attr_name in dir(YDB):
                attr = getattr(YDB, attr_name)
                if isinstance(attr, type) and hasattr(attr, 'python_type'):
                    if inner_type is attr:
                        return YDB.optional(attr)

    raise TypeError(f"Тип {t} не является допустимым YDB типом")


def ydb_dataclass(_cls=None, *, init=True, repr=True, eq=True, order=False):
    """Декоратор для создания dataclass с YDB типами"""

    def wrap(cls):
        # Собираем информацию о полях
        ydb_fields = {}
        python_annotations = {}
        field_defaults = {}

        # Получаем аннотации и значения по умолчанию
        annotations = get_type_hints(cls, include_extras=True)

        for name, type_annotation in annotations.items():
            if _is_ydb_type(type_annotation):
                ydb_type = _extract_ydb_type(type_annotation)
                ydb_fields[name] = YDBFieldInfo(name, ydb_type)
                python_annotations[name] = ydb_type.python_type

        # Обрабатываем значения по умолчанию
        for name, field in fields(cls):
            if name in ydb_fields and hasattr(cls, name):
                field_defaults[name] = getattr(cls, name)

        # Создаем dataclass с Python типами
        cls.__annotations__ = python_annotations

        # Устанавливаем значения по умолчанию
        for name, default_value in field_defaults.items():
            setattr(cls, name, default_value)

        cls = dataclass(cls, init=init, repr=repr, eq=eq, order=order)

        # Сохраняем YDB информацию
        cls._ydb_fields: ClassVar[Dict[str, YDBFieldInfo]] = ydb_fields
        cls._ydb_schema: ClassVar[Dict[str, str]] = None

        # Метод для получения схемы YDB
        @classmethod
        def get_ydb_schema(cls) -> Dict[str, str]:
            """Получить схему полей в формате YDB"""
            if cls._ydb_schema is None:
                cls._ydb_schema = {
                    name: field.ydb_type.ydb_type
                    for name, field in cls._ydb_fields.items()
                }
            return cls._ydb_schema.copy()

        # Конвертация в YDB значения
        def to_ydb_dict(self) -> Dict[str, Any]:
            """Конвертировать объект в словарь YDB значений"""
            result = {}
            for name, field in self._ydb_fields.items():
                value = getattr(self, name)
                result[name] = field.get_ydb_value(value)
            return result

        # Конвертация из YDB строки
        @classmethod
        def from_ydb_row(cls, row) -> Any:
            """Создать объект из строки YDB"""
            kwargs = {}

            for name, field in cls._ydb_fields.items():
                # Получаем значение из строки
                if hasattr(row, name):
                    raw_value = getattr(row, name)
                elif hasattr(row, '__getitem__'):
                    try:
                        raw_value = row[name]
                    except (KeyError, TypeError):
                        continue
                else:
                    continue

                # Конвертируем YDB значение в Python
                if raw_value is None:
                    kwargs[name] = None
                elif hasattr(raw_value, 'is_null') and raw_value.is_null():
                    kwargs[name] = None
                elif isinstance(raw_value, ydb.Null):
                    kwargs[name] = None
                elif hasattr(raw_value, 'int_value'):
                    kwargs[name] = raw_value.int_value
                elif hasattr(raw_value, 'uint_value'):
                    kwargs[name] = raw_value.uint_value
                elif hasattr(raw_value, 'utf8_value'):
                    kwargs[name] = raw_value.utf8_value
                elif hasattr(raw_value, 'bytes_value'):
                    kwargs[name] = raw_value.bytes_value
                elif hasattr(raw_value, 'double_value'):
                    kwargs[name] = raw_value.double_value
                elif hasattr(raw_value, 'bool_value'):
                    kwargs[name] = raw_value.bool_value
                elif hasattr(raw_value, 'uint64_value'):
                    kwargs[name] = raw_value.uint64_value
                elif hasattr(raw_value, 'int64_value'):
                    kwargs[name] = raw_value.int64_value
                elif hasattr(raw_value, 'microseconds'):
                    kwargs[name] = raw_value.microseconds
                elif hasattr(raw_value, 'text'):
                    kwargs[name] = raw_value.text
                else:
                    kwargs[name] = raw_value

            return cls(**kwargs)

        # Создание из словаря Python значений
        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> Any:
            """Создать объект из словаря Python значений"""
            kwargs = {}
            for name, field in cls._ydb_fields.items():
                if name in data:
                    kwargs[name] = data[name]
                elif field.default is not ...:
                    kwargs[name] = field.default
                else:
                    raise ValueError(f"Отсутствует обязательное поле: {name}")
            return cls(**kwargs)

        # Добавляем методы к классу
        cls.get_ydb_schema = get_ydb_schema
        cls.to_ydb_dict = to_ydb_dict
        cls.from_ydb_row = from_ydb_row
        cls.from_dict = from_dict

        return cls

    if _cls is None:
        return wrap
    return wrap(_cls)