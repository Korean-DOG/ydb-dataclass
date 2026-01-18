"""
YDB типы для аннотаций
"""

import ydb


class YDBType:
    """Базовый класс для YDB типов"""

    def __init__(self, python_type, ydb_type, ydb_value):
        self.python_type = python_type
        self.ydb_type = ydb_type
        self.ydb_value = ydb_value

    def __repr__(self):
        return f"YDB.{self.__class__.__name__}"

    def __call__(self, value):
        return self.ydb_value(value)


class YDBMeta(type):
    """Мета-класс для создания пространства имен YDB типов"""

    class int64(YDBType):
        """YDB Int64 тип"""

        def __init__(self):
            super().__init__(int, "Int64", ydb.Int64)

    class uint64(YDBType):
        """YDB Uint64 тип"""

        def __init__(self):
            super().__init__(int, "Uint64", ydb.Uint64)

    class utf8(YDBType):
        """YDB Utf8 тип"""

        def __init__(self):
            super().__init__(str, "Utf8", ydb.Utf8)

    class string(YDBType):
        """YDB String тип (bytes)"""

        def __init__(self):
            super().__init__(bytes, "String", ydb.String)

    class double(YDBType):
        """YDB Double тип"""

        def __init__(self):
            super().__init__(float, "Double", ydb.Double)

    class bool(YDBType):
        """YDB Bool тип"""

        def __init__(self):
            super().__init__(bool, "Bool", ydb.Bool)

    class timestamp(YDBType):
        """YDB Timestamp тип"""

        def __init__(self):
            super().__init__(int, "Timestamp", ydb.Timestamp)

    class json(YDBType):
        """YDB Json тип"""

        def __init__(self):
            super().__init__(str, "Json", ydb.Json)

    class date(YDBType):
        """YDB Date тип"""

        def __init__(self):
            super().__init__(int, "Date", ydb.Date)

    class datetime(YDBType):
        """YDB Datetime тип"""

        def __init__(self):
            super().__init__(int, "Datetime", ydb.Datetime)

    class interval(YDBType):
        """YDB Interval тип"""

        def __init__(self):
            super().__init__(int, "Interval", ydb.Interval)

    class decimal(YDBType):
        """YDB Decimal тип"""

        def __init__(self, precision=22, scale=9):
            self.precision = precision
            self.scale = scale
            super().__init__(
                str,
                f"Decimal({precision}, {scale})",
                lambda x: ydb.Decimal(x, precision, scale)
            )

    class Optional:
        """Обертка для Optional типов"""

        def __init__(self, inner_type):
            self.inner_type = inner_type
            self.python_type = self.inner_type.python_type
            self.ydb_type = f"Optional<{self.inner_type.ydb_type}>"
            self.ydb_value = self.inner_type.ydb_value

        def __repr__(self):
            return f"Optional[{self.inner_type}]"


class YDB(metaclass=YDBMeta):
    """Пространство имен YDB типов"""

    @staticmethod
    def optional(inner_type):
        """Создание Optional типа"""
        return YDB.Optional(inner_type)

    @staticmethod
    def decimal(precision=22, scale=9):
        """Создание Decimal типа с указанными параметрами"""
        return YDBMeta.decimal(precision, scale)