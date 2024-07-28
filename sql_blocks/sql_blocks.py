from enum import Enum

KEYWORD = {
    'SELECT': (',{}', 'SELECT *'),
    'FROM': ('{}', ''),
    'WHERE': ('{}AND ', ''),
    'GROUP BY': (',{}', ''),
    'ORDER BY': (',{}', ''),
    'LIMIT': (' ', ''),
}
SELECT, FROM, WHERE, GROUP_BY, ORDER_BY, LIMIT = KEYWORD.keys()
USUAL_KEYS = [SELECT, WHERE, GROUP_BY, ORDER_BY]

class SQLObject:
    def __init__(self, table_name: str=''):
        self.alias = ''
        self.values = {}
        self.key_field = ''
        self.set_table(table_name)

    def set_table(self, table_name: str):
        if not table_name:
            return
        if ' ' in table_name:
            table_name, self.alias = table_name.split()
        elif '_' in table_name:
            self.alias = ''.join(
                word[0].lower()
                for word in table_name.split('_')
            )
        else:
            self.alias = table_name.lower()[:3]
        self.values.setdefault(FROM, []).append(f'{table_name} {self.alias}')

    @property
    def table_name(self) -> str:
        return self.values[FROM][0].split()[0]
 
    def delete(self, search: str, keys: list=USUAL_KEYS):
        for key in keys:
            result = []
            for item in self.values.get(key, []):
                if search not in item:
                    result.append(item)
            self.values[key] = result


class Function:
    ...

class Field:
    prefix = ''

    @classmethod
    def format(cls, name: str, main: SQLObject) -> str:
        if name == '_':
            name = '*'
        else:
            name = f'{main.alias}.{name}'
        if Function in cls.__bases__:
            name = f'{cls.__name__}({name})'
        return f'{cls.prefix}{name}'

    @classmethod
    def add(cls, name: str, main: SQLObject):
        main.values.setdefault(SELECT, []).append(
            cls.format(name, main)
        )


class Avg(Function, Field):
    ...
class Min(Function, Field):
    ...
class Max(Function, Field):
    ...
class Sum(Function, Field):
    ...
class Count(Function, Field):
    ...

class Distinct(Field):
    prefix = 'DISTINCT '


class NamedField:
    def __init__(self, alias: str, class_type = Field):
        self.alias = alias
        if class_type not in [Field] + Field.__subclasses__():
            raise TypeError('class_type must be a Field (sub)class.')
        self.class_type = class_type

    def add(self, name: str, main: SQLObject):
        main.values.setdefault(SELECT, []).append(
            '{} as {}'.format(
                self.class_type.format(name, main),
                self.alias
            )
        )


class Table:
    def __init__(self, fields: list | str=[]):
        if isinstance(fields, str):
            fields = [f.strip() for f in fields.split(',')]
        self.fields = fields

    def add(self, name: str, main: SQLObject):
        main.set_table(name)
        for field in self.fields:
            Field.add(field, main)


class PrimaryKey:
    @staticmethod
    def add(name: str, main: SQLObject):
        main.key_field = name


class ForeignKey:
    references = {}

    def __init__(self, table_name: str):
        self.table_name = table_name

    def add(self, name: str, main: SQLObject):
        key = f'{main.table_name}_{self.table_name}'
        ForeignKey.references[key] = name

    @classmethod
    def find(cls, obj1: SQLObject, obj2: SQLObject) -> list:
        key = f'{obj1.table_name}_{obj2.table_name}'
        return cls.references.get(key, '').split('_')


class Where:
    def __init__(self, expr: str):
        self.expr = expr

    @classmethod
    def __constructor(cls, operator: str, value):
        if isinstance(value, str):
            return cls(expr=f"{operator} '{value}'")
        return cls(expr=f'{operator} {value}')

    @classmethod
    def eq(cls, value):
        return cls.__constructor('=', value)

    @classmethod
    def like(cls, value: str):
        return cls(f"LIKE '%{value}%'")
   
    @classmethod
    def gt(cls, value):
        return cls.__constructor('>', value)

    @classmethod
    def gte(cls, value):
        return cls.__constructor('>=', value)

    @classmethod
    def lt(cls, value):
        return cls.__constructor('<', value)

    @classmethod
    def lte(cls, value):
        return cls.__constructor('<=', value)
    
    @classmethod
    def is_null(cls):
        return cls('IS NULL')

    def add(self, name: str, main: SQLObject):
        main.values.setdefault(WHERE, []).append('{} {}'.format(
            Field.format(name, main), self.expr
        ))


class Not:
    def __init__(self, condition: Where):
        self.condition = condition
        """
            NOT NULL
            NOT LIKE '%   %'
            NOT in (...)
            value=Not(Where.eq(123)) # --->  'value <> 123'
        """


class Case:
    def __init__(self, default: str=''):
        self.__conditions = {}

    def when(self, result: str, condition: Where):
        self.__conditions.setdefault(result, []).append(condition)
        return self
    
    def add(self, name: str, main: SQLObject):
        Field.add(name, main)


class Options:
    def __init__(self, **values):
        self.__children: dict = values

    def add(self, logical_separator: str, main: SQLObject):
        """
        `logical_separator` must be AND or OR
        """
        conditions: list[str] = []
        child: Where
        for field, child in self.__children.items():
            conditions.append(' {} {} '.format(
                Field.format(field, main), child.expr
            ))
        main.values.setdefault(WHERE, []).append(
            '(' + logical_separator.join(conditions) + ')'
        )


class Between:
    def __init__(self, start, end):
        if start > end:
            start, end = end, start
        self.start = start
        self.end = end

    def add(self, name: str, main:SQLObject):
        Where.gte(self.start).add(name, main),
        Where.lte(self.end).add(name, main)


class SortType(Enum):
    ASC = ''
    DESC = ' DESC'

class OrderBy:
    sort: SortType = SortType.ASC
    @classmethod
    def add(cls, name: str, main: SQLObject):
        if main.alias:
            name = f'{main.alias}.{name}'
        main.values.setdefault(ORDER_BY, []).append(name + cls.sort.value)


class GroupBy:
    @staticmethod
    def add(name: str, main: SQLObject):
        main.values.setdefault(GROUP_BY, []).append(f'{main.alias}.{name}')


class Having:
    def __init__(self, function: Function, condition: Where):
        self.function = function
        self.condition = condition

    def add(self, name: str, main:SQLObject):
        main.values[GROUP_BY][-1] += ' HAVING {} {}'.format(
            self.function.format(name, main), self.condition.expr
        )
    
    @classmethod
    def avg(cls, condition: Where):
        return cls(Avg, condition)
    
    @classmethod
    def min(cls, condition: Where):
        return cls(Min, condition)
    
    @classmethod
    def max(cls, condition: Where):
        return cls(Max, condition)
    
    @classmethod
    def sum(cls, condition: Where):
        return cls(Sum, condition)
    
    @classmethod
    def count(cls, condition: Where):
        return cls(Count, condition)


class JoinType(Enum):
    INNER = ''
    LEFT = 'LEFT '
    RIGHT = 'RIGHT '
    FULL = 'FULL '

class Select(SQLObject):
    join_type: JoinType = JoinType.INNER
    REGEX = None

    def __init__(self, table_name: str='', **values):
        super().__init__(table_name)
        to_list = lambda x: x if isinstance(x, list) else [x]
        for name, params in values.items():
            for obj in to_list(params):
                obj.add(name, self)
        self.break_lines = True

    def add(self, name: str, main: SQLObject):
        def update_values(key: str, new_values: list):
            for value in new_values:
                old_values = main.values.get(key, [])
                if value not in old_values:
                    main.values[key] = old_values + [value]
        update_values(
            FROM, [
                '{jt}JOIN {tb} {a2} ON ({a1}.{f1} = {a2}.{f2})'.format(
                    jt=self.join_type.value,
                    tb=self.table_name,
                    a1=main.alias, f1=name,
                    a2=self.alias, f2=self.key_field
                )
            ] + self.values[FROM][1:]
        )
        for key in USUAL_KEYS:
            update_values(key, self.values.get(key, []))

    def __add__(self, other: SQLObject):
        foreign_field, *primary_key = ForeignKey.find(self, other)
        if not foreign_field:
            foreign_field, *primary_key = ForeignKey.find(other, self)
            if foreign_field:
                if primary_key:
                    PrimaryKey.add(primary_key[0], self)
                self.add(foreign_field, other)
                return other
            raise ValueError(f'No relationship found between {self.table_name} and {other.table_name}.')
        elif primary_key:
            PrimaryKey.add(primary_key[0], other)
        other.add(foreign_field, self)
        return self

    def __str__(self) -> str:
        TABULATION = '\n\t' if self.break_lines else ' '
        LINE_BREAK = '\n' if self.break_lines else ' '
        DEFAULT = lambda key: KEYWORD[key][1]
        FMT_SEP = lambda key: KEYWORD[key][0].format(TABULATION)
        select, _from, where, groupBy, orderBy, limit = [
            DEFAULT(key) if not self.values.get(key) else "{}{}{}{}".format(
                LINE_BREAK, key, TABULATION, FMT_SEP(key).join(self.values[key])
            ) for key in KEYWORD
        ]
        return f'{select}{_from}{where}{groupBy}{orderBy}{limit}'.strip()
   
    def __call__(self, **values):
        for name, obj in values.items():
            obj.add(name, self)
        return self
    
    def __eq__(self, other: SQLObject) -> bool:
        def sorted_values(obj: SQLObject, key: str) -> list:
            return sorted(obj.values.get(key, []))
        for key in KEYWORD:
            if sorted_values(self, key) != sorted_values(other, key):
                return False
        return True

    def limit(self, row_count: int, offset: int=0):
        result = [str(row_count)]
        if offset > 0:
            result.append(f'OFFSET {offset}')
        self.values.setdefault(LIMIT, result)
        return self

    @classmethod
    def parse(cls, txt: str) -> list[SQLObject]:
        import re
        txt = re.sub(' +', ' ', re.sub('\n|\t', ' ', txt))
        if not cls.REGEX:
            keywords = '|'.join(KEYWORD)
            cls.REGEX = re.compile(f'({keywords})', re.IGNORECASE)
        tokens = [t.strip() for t in cls.REGEX.split(txt) if t.strip()]
        values = {k: v for k, v in zip(tokens[::2], tokens[1::2])}
        tables = [t.strip() for t in re.split('JOIN|LEFT|RIGHT|ON', values[FROM]) if t.strip()]
        result = {}
        for item in tables:
            if '=' in item:
                a1, f1, a2, f2 = [r.strip() for r in re.split('[().=]', item) if r]
                obj1: SQLObject = result[a1]
                obj2:SQLObject = result[a2]
                PrimaryKey.add(f2, obj2)
                ForeignKey(obj2.table_name).add(f1, obj1)
            else:
                obj = cls(item)
                for key in USUAL_KEYS:
                    if not key in values:
                        continue
                    separator = KEYWORD[key][0].format(
                        ' ' if key == WHERE else ''
                    )
                    fields = [
                        fld.strip() for fld in re.split(
                            separator, values[key]
                        ) if len(tables) == 1
                        or re.findall(f'^[( ]*{obj.alias}\.', fld)
                    ]
                    obj.values[key] = [
                        f'{obj.alias}.{f}' if not '.' in f else f for f in fields
                    ]
                result[obj.alias] = obj
        return list( result.values() )


class SubSelect(Select):
    def add(self, name: str, main: SQLObject):
        self.break_lines = False
        Where(f'IN ({self})').add(name, main)
