from enum import Enum
import re


PATTERN_PREFIX = '([^0-9 ]+[.])'
PATTERN_SUFFIX = '( [A-Za-z_]+)'
DISTINCT_PREFX = '(DISTINCT|distinct)'

KEYWORD = {
    'SELECT':   (',{}',     DISTINCT_PREFX),
    'FROM':     ('{}',      PATTERN_SUFFIX),
    'WHERE':    ('{}AND ',  ''),
    'GROUP BY': (',{}',     PATTERN_SUFFIX),
    'ORDER BY': (',{}',     PATTERN_SUFFIX),
    'LIMIT':    (' ',       ''),
}                                                    
#                  ^          ^
#                  |          |
#                  |          +----- pattern to compare fields
#                  |
#                  +-------- separator

SELECT, FROM, WHERE, GROUP_BY, ORDER_BY, LIMIT = KEYWORD.keys()
USUAL_KEYS = [SELECT, WHERE, GROUP_BY, ORDER_BY, LIMIT]
TO_LIST = lambda x: x if isinstance(x, list) else [x]


class SQLObject:
    ALIAS_FUNC = None
    """    ^^^^^^^^^^^^^^^^^^^^^^^^
    You can change the behavior by assigning 
    a user function to SQLObject.ALIAS_FUNC
    """

    def __init__(self, table_name: str=''):
        self.__alias = ''
        self.values = {}
        self.key_field = ''
        self.set_table(table_name)

    @classmethod
    def split_alias(cls, table_name: str) -> tuple:
        is_file_name = any([
            '/' in table_name, '.' in table_name
        ])
        ref = table_name
        if is_file_name:
            ref = table_name.split('/')[-1].split('.')[0]
        if cls.ALIAS_FUNC:
            return cls.ALIAS_FUNC(ref), table_name
        elif ' ' in table_name.strip():
            table_name, alias = table_name.split()
            return alias, table_name
        elif '_' in ref:
            return ''.join(
                word[0].lower()
                for word in ref.split('_')
            ), table_name
        return ref.lower()[:3], table_name

    def set_table(self, table_name: str):
        if not table_name:
            return
        self.__alias, table_name = self.split_alias(table_name)
        self.values.setdefault(FROM, []).append(f'{table_name} {self.alias}')

    @property
    def table_name(self) -> str:
        return self.values[FROM][0].split()[0]
    
    def set_file_format(self, pattern: str):
        if '{' not in pattern:
            pattern = '{}' + pattern
        self.values[FROM][0] = pattern.format(self.aka())

    @property
    def alias(self) -> str:
        if self.__alias:
            return self.__alias
        return self.table_name
 
    @staticmethod
    def get_separator(key: str) -> str:
        if key == WHERE:
            return r'\s+and\s+|\s+AND\s+'
        appendix = {FROM: r'\s+join\s+|\s+JOIN\s+'}
        return KEYWORD[key][0].format(appendix.get(key, ''))

    @staticmethod
    def contains_CASE_statement(text: str) -> bool:
        return re.search(r'\bCASE\b', text, re.IGNORECASE)

    @classmethod
    def split_fields(cls, text: str, key: str) -> list:
        if key == SELECT and cls.contains_CASE_statement(text):
            return Case.parse(text)
        text = re.sub(r'\s+', ' ', text)
        separator = cls.get_separator(key)
        return re.split(separator, text)

    @staticmethod
    def is_named_field(fld: str, name: str='') -> bool:
        return re.search(fr'(\s+as\s+|\s+AS\s+){name}', fld)

    def has_named_field(self, name: str) -> bool:
        return any(
            self.is_named_field(fld, name)
            for fld in self.values.get(SELECT, [])
        )

    def diff(self, key: str, search_list: list, exact: bool=False) -> set:
        def disassemble(source: list) -> list:
            if not exact:
                return source
            result = []
            for fld in source:
                result += re.split(r'([=()]|<>|\s+ON\s+|\s+on\s+)', fld)
            return result
        def cleanup(text: str) -> str:
            # if re.search(r'^CASE\b', text):
            if self.contains_CASE_statement(text):
                return text
            text = re.sub(r'[\n\t]', ' ', text)
            if exact:
                text = text.lower()
            return text.strip()
        def field_set(source: list) -> set:
            return set(
                (
                    fld 
                    if key == SELECT and self.is_named_field(fld, key) 
                    else
                    re.sub(pattern, '', cleanup(fld))
                )
                for string in disassemble(source)
                for fld in self.split_fields(string, key)
            )       
        pattern = KEYWORD[key][1] 
        if exact:
            if key == WHERE:
                pattern = r'["\']| '
            pattern += f'|{PATTERN_PREFIX}'
        s1 = field_set(search_list)
        s2 = field_set(self.values.get(key, []))
        if exact:
            return s1.symmetric_difference(s2)
        return s1 - s2

    def delete(self, search: str, keys: list=USUAL_KEYS, exact: bool=False):
        search = re.escape(search)
        if exact:
            not_match = lambda item: not re.search(fr'\w*[.]*{search}$', item)
        else:
            not_match = lambda item: search not in item
        for key in keys:
            self.values[key] = [
                item for item in self.values.get(key, [])
                if not_match(item)
            ]


SQL_CONST_SYSDATE = 'SYSDATE'
SQL_CONST_CURR_DATE = 'Current_date'
SQL_ROW_NUM = 'ROWNUM'
SQL_CONSTS = [SQL_CONST_SYSDATE, SQL_CONST_CURR_DATE, SQL_ROW_NUM]


class Field:
    prefix = ''

    @classmethod
    def format(cls, name: str, main: SQLObject) -> str:
        def is_const() -> bool:
            return any([
                re.findall('[.()0-9]', name),
                name in SQL_CONSTS,
                re.findall(r'\w+\s*[+-]\s*\w+', name)
            ])
        name = name.strip()
        if name in ('_', '*'):
            name = '*'
        elif not is_const() and not main.has_named_field(name):
            name = f'{main.alias}.{name}'
        if Function in cls.__bases__:
            name = f'{cls.__name__}({name})'
        return f'{cls.prefix}{name}'

    @classmethod
    def add(cls, name: str, main: SQLObject):
        main.values.setdefault(SELECT, []).append(
            cls.format(name, main)
        )


class Distinct(Field):
    prefix = 'DISTINCT '


class NamedField:
    def __init__(self, alias: str, class_type = Field):
        self.alias = alias
        self.class_type = class_type

    def add(self, name: str, main: SQLObject):
        main.values.setdefault(SELECT, []).append(
            '{} as {}'.format(
                self.class_type.format(name, main),
                self.alias  # --- field alias
            )
        )


class Code:
    def __init__(self):
        # --- Replace class method by instance method: ------
        self.add = self.__add
        # -----------------------------------------------------
        self.field_class = Field
        self.extra = {}

    def As(self, field_alias: str, modifiers=None):
        if modifiers:
            self.extra[field_alias] = TO_LIST(modifiers)
        if field_alias:
            self.field_class = NamedField(field_alias)
        return self
    
    def format(self, name: str, main: SQLObject) -> str:
        return Field.format(name, main)

    def __add(self, name: str, main: SQLObject):
        name = self.format(name, main)
        self.field_class.add(name, main)
        if self.extra:
            main.__call__(**self.extra)

    @classmethod
    def add(cls, name: str, main: SQLObject):
        cls().__add(name, main)


class Dialect(Enum):
    ANSI = 0
    SQL_SERVER = 1
    ORACLE = 2
    POSTGRESQL = 3
    MYSQL = 4

SQL_TYPES = 'CHAR INT DATE FLOAT ANY'.split()
CHAR, INT, DATE, FLOAT, ANY  =  SQL_TYPES

class Function(Code):
    dialect = Dialect.ANSI
    inputs = None
    output = None
    separator = ', '
    auto_convert = True
    append_param = False

    def __init__(self, *params: list):
        def set_func_types(param):
            if self.auto_convert and isinstance(param, Function):
                func = param
                main_param = self.inputs[0]
                unfriendly = all([
                    func.output != main_param,
                    func.output != ANY,
                    main_param  != ANY
                ])
                if unfriendly:
                    return Cast(func, main_param)
            return param
        self.params = [set_func_types(p) for p in params]
        self.pattern = self.get_pattern()
        super().__init__()

    @classmethod
    def descendants(cls) -> list:
        result = []
        for sub in cls.__subclasses__():
            result.append(sub)
            result += sub.descendants()
        return result
    
    def get_pattern(self) -> str:
        return '{func_name}({params})'

    def __str__(self) -> str:
        return self.pattern.format(
            func_name=self.__class__.__name__,
            params=self.separator.join(str(p) for p in self.params)
        )

    @classmethod
    def help(cls) -> str:
        descr = ' '.join(B.__name__ for B in cls.__bases__)
        params = cls.inputs or ''
        return cls().get_pattern().format(
            func_name=f'{descr} {cls.__name__}',
            params=cls.separator.join(str(p) for p in params)
        ) + f'  Return {cls.output}'

    def set_main_param(self, name: str, main: SQLObject) -> bool:
        nested_functions = [
            param for param in self.params if isinstance(param, Function)
        ]
        for func in nested_functions:
            if func.inputs:
                func.set_main_param(name, main)
                return
        new_params = [Field.format(name, main)]
        if self.append_param:
            self.params += new_params
        else:
            self.params = new_params + self.params         

    def format(self, name: str, main: SQLObject) -> str:
        if name not in '*_':
            self.set_main_param(name, main)
        return str(self)



# ---- String Functions: ---------------------------------
class SubString(Function):
    inputs = [CHAR, INT, INT]
    output = CHAR

    def get_pattern(self) -> str:
        if self.dialect in (Dialect.ORACLE, Dialect.MYSQL):
            return 'Substr({params})'
        return super().get_pattern()

# ---- Numeric Functions: --------------------------------
class Round(Function):
    inputs = [FLOAT]
    output = FLOAT

class Trunc(Function):
    inputs = [FLOAT]
    output = FLOAT

# --- Date Functions: ------------------------------------
class DateDiff(Function):
    inputs = [DATE]
    output = DATE
    append_param = True

    def __str__(self) -> str:
        def is_field_or_func(obj) -> bool:
            if not isinstance(obj, str):
                return True
            candidate = re.sub(
                '[()]', '', obj.split('.')[-1]
            )
            return candidate.isidentifier()
        self.params = [
            p if is_field_or_func(p) else f"'{p}'"
            for p in self.params
        ]
        if self.dialect != Dialect.SQL_SERVER:
            return ' - '.join(
                str(p) for p in self.params
            )  # <====  Date subtract
        return super().__str__()


class DatePart(Function):
    inputs = [DATE]
    output = INT

    def get_pattern(self) -> str:
        interval = self.__class__.__name__
        database_type = {
            Dialect.ORACLE: 'Extract('+interval+' FROM {params})',
            Dialect.POSTGRESQL: "Date_Part('"+interval+"', {params})",
        }
        if self.dialect in database_type:
            return database_type[self.dialect]
        return super().get_pattern()

class Year(DatePart):
    ...
class Month(DatePart):
    ...
class Day(DatePart):
    ...


class Current_Date(Function):
    output = DATE

    def get_pattern(self) -> str:
        database_type = {
            Dialect.ORACLE: SQL_CONST_SYSDATE,
            Dialect.POSTGRESQL: SQL_CONST_CURR_DATE,
            Dialect.SQL_SERVER: 'getDate()'
        }
        if self.dialect in database_type:
            return database_type[self.dialect]
        return super().get_pattern()
# --------------------------------------------------------

class Frame:
    break_lines: bool = True

    def over(self, **args):
        """
        How to use:
            over(field1=OrderBy, field2=Partition)
        """
        keywords = ''
        for field, obj in args.items():
            is_valid = any([
                obj is OrderBy, obj is DescOrderBy,
                obj is Partition,
                isinstance(obj, Rows),
            ])
            if not is_valid:
                continue
            keywords += '{}{}'.format(
                '\n\t\t' if self.break_lines else ' ',
                obj.cls_to_str(field if field != '_' else '')
            )
        if keywords and self.break_lines:
            keywords += '\n\t'
        self.pattern = self.get_pattern() + f' OVER({keywords})'
        return self


class Aggregate(Frame):
    inputs = [FLOAT]
    output = FLOAT

class Window(Frame):
    ...

# ---- Aggregate Functions: -------------------------------
class Avg(Aggregate, Function):
    ...
class Min(Aggregate, Function):
    ...
class Max(Aggregate, Function):
    ...
class Sum(Aggregate, Function):
    ...
class Count(Aggregate, Function):
    ...

# ---- Window Functions: -----------------------------------
class Row_Number(Window, Function):
    output = INT

class Rank(Window, Function):
    output = INT

class Lag(Window, Function):
    output = ANY

class Lead(Window, Function):
    output = ANY


# ---- Conversions and other Functions: ---------------------
class Coalesce(Function):
    inputs = [ANY]
    output = ANY
    
class Cast(Function):
    inputs = [ANY]
    output = ANY
    separator = ' As '


FUNCTION_CLASS = {f.__name__.lower(): f for f in Function.descendants()}


class ExpressionField:
    def __init__(self, expr: str):
        self.expr = expr

    def add(self, name: str, main: SQLObject):
        main.values.setdefault(SELECT, []).append(self.format(name, main))

    def format(self, name: str, main: SQLObject) -> str:
        """
        Replace special chars...
            {af}  or  {a.f} or % = alias and field
            {a} = alias
            {f} = field
            {t} = table name
        """
        return re.sub('{af}|{a.f}|[%]', '{a}.{f}', self.expr).format(
            a=main.alias, f=name, t=main.table_name
        )

class FieldList:
    separator = ','

    def __init__(self, fields: list=[], class_types = [Field], ziped: bool=False):
        if isinstance(fields, str):
            fields = [
                f.strip() for f in fields.split(self.separator)
            ]
        self.fields = fields
        self.class_types = class_types
        self.ziped = ziped

    def add(self, name: str, main: SQLObject):
        if self.ziped:  # --- One class per field...
            for field, class_type in zip(self.fields, self.class_types):
                class_type.add(field, main)
            return
        for field in self.fields:
            for class_type in self.class_types:
                class_type.add(field, main)


class Table(FieldList):
    def add(self, name: str, main: SQLObject):
        main.set_table(name)
        super().add(name, main)


class PrimaryKey:
    @staticmethod
    def add(name: str, main: SQLObject):
        main.key_field = name


class ForeignKey:
    references = {}

    def __init__(self, table_name: str):
        self.table_name = table_name

    @staticmethod
    def get_key(obj1: SQLObject, obj2: SQLObject) -> tuple:
        # [To-Do] including alias will allow to relate the same table twice
        return obj1.table_name, obj2.table_name

    def add(self, name: str, main: SQLObject):
        key = self.get_key(main, self)
        ForeignKey.references[key] = (name, '')

    @classmethod
    def find(cls, obj1: SQLObject, obj2: SQLObject) -> tuple:
        key = cls.get_key(obj1, obj2)
        a, b = cls.references.get(key, ('', ''))
        return a, (b or obj2.key_field)


def quoted(value) -> str:
    if isinstance(value, str):
        if re.search(r'\bor\b', value, re.IGNORECASE):
            raise PermissionError('Possible SQL injection attempt')
        value = f"'{value}'"
    return str(value)


class Position(Enum):
    StartsWith = -1
    Middle = 0
    EndsWith = 1


class Where:
    prefix = ''

    def __init__(self, content: str):
        self.content = content

    @classmethod
    def __constructor(cls, operator: str, value):
        return cls(f'{operator} {quoted(value)}')

    @classmethod
    def eq(cls, value):
        return cls.__constructor('=', value)

    @classmethod
    def contains(cls, text: str, pos: int | Position = Position.Middle):
        if isinstance(pos, int):
            pos = Position(pos)
        return cls(
            "LIKE '{}{}{}'".format(
                '%' if pos != Position.StartsWith else '',
                text,
                '%' if pos != Position.EndsWith else ''
            )
        )
   
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
    
    @classmethod
    def inside(cls, values):
        if isinstance(values, list):
            values = ','.join(quoted(v) for v in values)
        return cls(f'IN ({values})')

    @classmethod
    def formula(cls, formula: str):
        where = cls( ExpressionField(formula) )
        where.add = where.add_expression
        return where

    def add_expression(self, name: str, main: SQLObject):
        self.content = self.content.format(name, main)
        main.values.setdefault(WHERE, []).append('{} {}'.format(
            self.prefix, self.content
        ))

    @classmethod
    def join(cls, query: SQLObject, pairs: dict=None):
        where = cls(query)
        where.pairs = pairs
        where.add = where.add_join
        return where

    def add_join(self, name: str, main: SQLObject):
        query = self.content
        main.values[FROM].append(f',{query.table_name} {query.alias}')
        for key in USUAL_KEYS:
            main.update_values(key, query.values.get(key, []))
        if not self.pairs:
            if not query.key_field:
                return
            self.pairs = {name: query.key_field}
        a1, a2 = main.alias, query.alias
        main.has_named_field
        for f1, f2 in self.pairs.items():
            if main.has_named_field(f1):
                expr = f'({a2}.{f2} = {f1})'
            else:
                expr = f'({a2}.{f2} = {a1}.{f1})'
            main.values.setdefault(WHERE, []).append(expr)

    def add(self, name: str, main: SQLObject):
        func_type = FUNCTION_CLASS.get(name.lower())
        if func_type:
            name = func_type.format('*', main)
        elif not main.has_named_field(name):
            name = Field.format(name, main)
        main.values.setdefault(WHERE, []).append('{}{} {}'.format(
            self.prefix, name, self.content
        ))


eq, contains, gt, gte, lt, lte, is_null, inside = (
    getattr(Where, method) for method in 
    ('eq', 'contains', 'gt', 'gte', 'lt', 'lte', 'is_null', 'inside')
) 
startswith, endswith = [
    lambda x: contains(x, Position.StartsWith),
    lambda x: contains(x, Position.EndsWith)
]


class Not(Where):
    prefix = 'NOT '

    @classmethod
    def eq(cls, value):
        return Where(f'<> {quoted(value)}')


class Case:
    break_lines = True

    def __init__(self, field: str):
        self.__conditions = {}
        self.default = None
        self.field = field
        self.current_condition = None
        self.fields = []

    def when(self, condition: Where, result=None):
        self.current_condition = condition
        if result is None:            
            return self
        return self.then(result)
    
    def then(self, result):
        if isinstance(result, str):
            if result.startswith('='):
                result = result[1:]
            else:
                result = quoted(result)
        self.__conditions[result] = self.current_condition
        return self
    
    def else_value(self, default):
        if isinstance(default, str):
            default = quoted(default)
        self.default = default
        return self
    
    def format(self, name: str, field: str='') -> str:
        TABULATION = '\t\t' if self.break_lines else ' '
        LINE_BREAK = '\n' if self.break_lines else ' '
        default = self.default
        if not field:
            field = self.field
        return 'CASE{brk}{cond}{df}{tab}END{alias}'.format(
            brk=LINE_BREAK,
            cond=LINE_BREAK.join(
                f'{TABULATION}WHEN {field} {cond.content} THEN {res}'
                for res, cond in self.__conditions.items()
            ),
            df=f'{LINE_BREAK}{TABULATION}ELSE {default}' if not default is None else '',
            tab='\n\t' if self.break_lines else ' ',
            alias=f' AS {name}' if name else ''
        )
    
    def __str__(self):
        return self.format('', self.field)

    def add(self, name: str, main: SQLObject):
        main.values.setdefault(SELECT, []).append(
            self.format(
                name, Field.format(self.field, main)
            )
        )

    @classmethod
    def parse(cls, expr: str) -> list:
        result = []
        block: 'Case' = None        
        # ---- functions of keywords: -----------------
        def _when(word: str):
            field, condition = word.split(' ', maxsplit=1)
            condition = Where(condition)
            if not block:
                return cls(field).when(condition)
            return block.when(condition)
        def _then(word: str):
            return block.then( eval(word) )
        def _else(word: str):
            return block.else_value( eval(word) )
        def _end(word: str):
            name, *rest = [t.strip() for t in re.split(r'\s+AS\s+|[,]', word) if t]
            block.fields.append(
                block.format(name)
            )
            block.fields += rest
            return block
        # -------------------------------------------------
        KEYWORDS = {
            'WHEN': _when, 'THEN': _then,
            'ELSE': _else, 'END':  _end,
        }
        RESERVED_WORDS = ['CASE'] + list(KEYWORDS)
        REGEX = '|'.join(fr'\b{word}\b' for word in RESERVED_WORDS)
        expr = re.sub(r'\s+', ' ', expr)
        tokens = [t for t in re.split(f'({REGEX})', expr) if t.strip()]
        last_word = ''
        while tokens:
            word = tokens.pop(0)
            if last_word in KEYWORDS:
                try:
                    block = KEYWORDS[last_word](word)
                except:
                    break
                result += block.fields
                block.fields = []
            elif word not in RESERVED_WORDS:
                result.append(word.replace(',', ''))
            last_word = word
        return result


class If(Code, Frame):
    """
    Behaves like an aggregation function
    """
    def __init__(self, field: str, func_class: Function, condition: Where=None):
        if not condition:
            field, *elements = re.split(r'([<>=]|\bin\b|\blike\b)', field, re.IGNORECASE)
            condition = Where( ''.join(elements) )
        self.field = field.strip()
        self.condition = condition
        self.func_class = func_class
        self.pattern = ''
        super().__init__()

    def format(self, name: str, main: SQLObject) -> str:
        return '{func}({param}){over}'.format(
            func=self.func_class.__name__,
            param=Case(self.field).when(self.condition, f'={name}').else_value(0),
            over=self.pattern
        )
    
    def get_pattern(self) -> str:
        return ''


class Options:
    def __init__(self, **values):
        self.__children: dict = values

    def add(self, logical_separator: str, main: SQLObject):
        if logical_separator.upper() not in ('AND', 'OR'):
            raise ValueError('`logical_separator` must be AND or OR')
        temp = Select(f'{main.table_name} {main.alias}')
        child: Where
        for field, child in self.__children.items():
            child.add(field, temp)
        main.values.setdefault(WHERE, []).append(
            '(' + f'\n\t{logical_separator} '.join(temp.values[WHERE]) + ')'
        )


class Between:
    is_literal: bool = False

    def __init__(self, start, end):
        if start > end:
            start, end = end, start
        self.start = start
        self.end = end

    def literal(self) -> Where:
        return Where('BETWEEN {} AND {}'.format(
            self.start, self.end
        ))

    def add(self, name: str, main:SQLObject):
        if self.is_literal:
            return self.literal().add(name, main)
        Where.gte(self.start).add(name, main)
        Where.lte(self.end).add(name, main)

class SameDay(Between):
    def __init__(self, date: str):
        super().__init__(
            f'{date} 00:00:00',
            f'{date} 23:59:59',
        )


class Range(Case):
    INC_FUNCTION = lambda x: x + 1

    def __init__(self, field: str, values: dict):
        super().__init__(field)
        start = 0
        cls = self.__class__
        for label, value in sorted(values.items(), key=lambda item: item[1]):
            self.when(
                Between(start, value).literal(), label
            )
            start = cls.INC_FUNCTION(value)


class Clause:
    @classmethod
    def format(cls, name: str, main: SQLObject) -> str:
        def is_function() -> bool:
            diff = main.diff(SELECT, [name.lower()], True)
            return diff.intersection(FUNCTION_CLASS)
        found = re.findall(r'^_\d', name)
        if found:
            name = found[0].replace('_', '')
        elif '.' not in name and main.alias and not is_function():
            name = f'{main.alias}.{name}'
        return name


class SortType(Enum):
    ASC = ''
    DESC = ' DESC'

class Row:
    def __init__(self, value: int=0):
        self.value = value

    def __str__(self) -> str:
        return '{} {}'.format(
            'UNBOUNDED' if self.value == 0 else self.value,
            self.__class__.__name__.upper()
        )

class Preceding(Row):
    ...
class Following(Row):
    ...
class Current(Row):
    def __str__(self) -> str:
        return 'CURRENT ROW'

class Rows:
    def __init__(self, *rows: list[Row]):
        self.rows = rows

    def cls_to_str(self, field: str='') -> str:
        return 'ROWS {}{}'.format(
            'BETWEEN ' if len(self.rows) > 1 else '',
            ' AND '.join(str(row) for row in self.rows)
        )


class DescOrderBy:
    @classmethod
    def add(cls, name: str, main: SQLObject):
        name = Clause.format(name, main)
        main.values.setdefault(ORDER_BY, []).append(name + SortType.DESC.value)

    @classmethod
    def cls_to_str(cls, field: str='') -> str:
        return f"{ORDER_BY} {field} DESC"


class OrderBy(Clause):
    sort: SortType = SortType.ASC
    DESC = DescOrderBy

    @classmethod
    def add(cls, name: str, main: SQLObject):
        name = cls.format(name, main)
        main.values.setdefault(ORDER_BY, []).append(name+cls.sort.value)

    @staticmethod
    def ascending(value: str) -> bool:
        if re.findall(r'\s+(DESC)\s*$', value):
            return False
        return True

    @classmethod
    def format(cls, name: str, main: SQLObject) -> str:
        if cls.ascending(name):
            cls.sort = SortType.ASC
        else:
            cls.sort = SortType.DESC
        return super().format(name, main)

    @classmethod
    def cls_to_str(cls, field: str='') -> str:
        return f"{ORDER_BY} {field}"

class Partition:
    @classmethod
    def cls_to_str(cls, field: str) -> str:
        return f'PARTITION BY {field}'


class GroupBy(Clause):
    def __init__(self, **args):
        # --- Replace class method by instance method: ------
        self.add = self.__add
        # -----------------------------------------------------
        self.args = args

    def __add(self, name: str, main: SQLObject):        
        func: Function = None
        fields = []
        for alias, obj in self.args.items():
            if isinstance(obj, type) and obj in Function.descendants():
                func: Function = obj
                name = func().format(name, main)
                NamedField(alias).add(name, main)
                fields += [alias]
            elif isinstance(obj, Aggregate):
                obj.As(alias).add('', main)
            elif isinstance(obj, Select):
                query: Select = obj
                fields += query.values.get(SELECT, [])
                query.add(alias, main)
            elif obj == Field:
                fields += [alias]
        if not func:
            fields += [self.format(name, main)]
        for field in fields:
            field = re.split(r'\s+(AS|as)\s+', field)[-1]
            main.values.setdefault(GROUP_BY, []).append(field)

    @classmethod
    def add(cls, name: str, main: SQLObject):
        cls().__add(name, main)


class Having:
    def __init__(self, function: Function, condition: Where):
        self.function = function
        self.condition = condition

    def add(self, name: str, main:SQLObject):
        main.values[GROUP_BY][-1] += ' HAVING {} {}'.format(
            self.function().format(name, main), self.condition.content
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


class Rule:
    @classmethod
    def apply(cls, target: 'Select'):
        ...

class QueryLanguage:
    pattern = '{select}{_from}{where}{group_by}{order_by}{limit}'
    has_default = {key: bool(key == SELECT) for key in KEYWORD}

    @staticmethod
    def remove_alias(text: str) -> str:
        value, sep = '', ''
        text = re.sub('[\n\t]', ' ', text)
        if ':' in text:
            text, value = text.split(':', maxsplit=1)
            sep = ':'
        return '{}{}{}'.format(
            ''.join(re.split(r'\w+[.]', text)),
            sep, value.replace("'", '"')
        )

    def join_with_tabs(self, values: list, sep: str='') -> str:
        sep = sep + self.TABULATION
        return sep.join(v for v in values if v)

    def add_field(self, values: list) -> str:
        if not values:
            return '*'
        return  self.join_with_tabs(values, ',')

    def get_tables(self, values: list) -> str:
        return  self.join_with_tabs(values)

    def extract_conditions(self, values: list) -> str:
        return  self.join_with_tabs(values, ' AND ')

    def sort_by(self, values: list) -> str:
        if OrderBy.sort == SortType.DESC and OrderBy.ascending(values[-1]):
            values[-1] += ' DESC'
        return self.join_with_tabs(values, ',')

    def set_group(self, values: list) -> str:
        return  self.join_with_tabs(values, ',')

    def set_limit(self, values: list) -> str:
        return self.join_with_tabs(values, ' ')

    def __init__(self, target: 'Select'):
        self.KEYWORDS = [SELECT, FROM, WHERE, GROUP_BY, ORDER_BY, LIMIT]
        self.TABULATION = '\n\t' if target.break_lines else ' '
        self.LINE_BREAK = '\n' if target.break_lines else ' '
        self.TOKEN_METHODS = {
            SELECT: self.add_field, FROM: self.get_tables, 
            WHERE: self.extract_conditions, LIMIT: self.set_limit,
            ORDER_BY: self.sort_by, GROUP_BY: self.set_group,
        }
        self.result = {}
        self.target = target

    def pair(self, key: str) -> str:
        if key == FROM:
            return '_from'
        return key.lower().replace(' ', '_')

    def prefix(self, key: str) -> str:
        return self.LINE_BREAK + key + self.TABULATION

    def convert(self) -> str:
        for key in self.KEYWORDS:
            method = self.TOKEN_METHODS.get(key)
            ref = self.pair(key)
            values = self.target.values.get(key, [])
            if not method or (not values and not self.has_default[key]):
                self.result[ref] = ''
                continue
            if key == FROM:
                values[0] = '{} {}'.format(
                    self.target.aka(), self.target.alias
                ).strip()
            text = method(values)
            self.result[ref] = self.prefix(key) + text
        return self.pattern.format(**self.result).strip()

class MongoDBLanguage(QueryLanguage):
    pattern = '{_from}.{function}({where}{select}{group_by}){order_by}'
    has_default = {key: False for key in KEYWORD}
    LOGICAL_OP_TO_MONGO_FUNC = {
        '>': '$gt',  '>=': '$gte',
        '<': '$lt',  '<=': '$lte',
        '=': '$eq',  '<>': '$ne', 
        'like': '$regex', 'LIKE': '$regex',
    }
    OPERATORS = '|'.join(op for op in LOGICAL_OP_TO_MONGO_FUNC)
    REGEX = {
        'options': re.compile(r'\s+or\s+|\s+OR\s+'),
        'condition': re.compile(fr'({OPERATORS})')
    }

    def join_with_tabs(self, values: list, sep: str=',') -> str:
        def format_field(fld):
            return '{indent}{fld}'.format(
                fld=self.remove_alias(fld),
                indent=self.TABULATION
            )
        return '{begin}{content}{line_break}{end}'.format(
            begin='{',
            content= sep.join(
                format_field(fld) for fld in values if fld
            ),
            end='}', line_break=self.LINE_BREAK,
        )

    def add_field(self, values: list) -> str:
        if self.result['function'] == 'aggregate':
            return ''
        return ',{content}'.format(
            content=self.join_with_tabs([f'{fld}: 1' for fld in values]),
        )

    def get_tables(self, values: list) -> str:
        return values[0].split()[0].lower()

    @classmethod
    def mongo_where_list(cls, values: list) -> list:
        OR_REGEX = cls.REGEX['options']
        where_list = []
        for condition in values:
            if OR_REGEX.findall(condition):
                condition = re.sub('[()]', '', condition)
                expr = '{begin}$or: [{content}]{end}'.format(
                    content=','.join(
                        cls.mongo_where_list( OR_REGEX.split(condition) )
                    ), begin='{', end='}',
                )
                where_list.append(expr)
                continue
            tokens = cls.REGEX['condition'].split( 
                cls.remove_alias(condition) 
            )
            tokens = [t.strip() for t in tokens if t]
            field, *op, const = tokens
            op = ''.join(op)
            expr = '{begin}{op}:{const}{end}'.format(
                begin='{', const=const.replace('%', '.*'), end='}',
                op=cls.LOGICAL_OP_TO_MONGO_FUNC[op],                
            )
            where_list.append(f'{field}:{expr}')
        return where_list
    
    def extract_conditions(self, values: list) -> str:
        return self.join_with_tabs(
            self.mongo_where_list(values)
        )

    def sort_by(self, values: list) -> str:
        return  ".sort({begin}{indent}{field}:{flag}{line_break}{end})".format(
            begin='{', field=self.remove_alias(values[0].split()[0]), 
            flag=-1 if OrderBy.sort == SortType.DESC else 1,
            end='}', indent=self.TABULATION, line_break=self.LINE_BREAK,
        )

    def set_group(self, values: list) -> str:
        self.result['function'] = 'aggregate'
        return '{"$group" : {_id:"$%%", count:{$sum:1}}}'.replace(
            '%%', self.remove_alias( values[0] )
        )
    
    def __init__(self, target: 'Select'):
        super().__init__(target)
        self.result['function'] = 'find'
        self.KEYWORDS = [GROUP_BY, SELECT, FROM, WHERE, ORDER_BY]

    def prefix(self, key: str):
        return ''


class Neo4JLanguage(QueryLanguage):
    pattern = 'MATCH {_from}{where}RETURN {select}{order_by}'
    has_default = {WHERE: False, FROM: False, ORDER_BY: True, SELECT: True}

    def add_field(self, values: list) -> str:
        if values:
            return self.join_with_tabs(values, ',')
        return self.TABULATION + ','.join(self.aliases.keys())

    def get_tables(self, values: list) -> str:
        NODE_FORMAT = dict(
            left='({}:{}{})<-',
            core='[{}:{}{}]',
            right='->({}:{}{})'
        )
        nodes = {k: '' for k in NODE_FORMAT}
        for txt in values:
            found = re.search(
                r'^(left|right)\s+', txt, re.IGNORECASE
            )
            pos, end, i = 'core', 0, 0
            if found:
                start, end = found.span()
                pos = txt[start:end-1].lower()
                i = 1
            tokens = re.split(r'JOIN\s+|ON\s+', txt[end:])
            txt = tokens[i].strip()
            table_name, *alias = txt.split()
            if alias:
                alias = alias[0]
            else:
                alias = SQLObject.ALIAS_FUNC(table_name)
            condition = self.aliases.get(alias, '')
            if not condition:
                self.aliases[alias] = ''
            nodes[pos] = NODE_FORMAT[pos].format(alias, table_name, condition)
        return self.TABULATION + '{left}{core}{right}'.format(**nodes)
        

    def extract_conditions(self, values: list) -> str:
        equalities = {}
        where_list = []
        for condition in values:
            other_comparisions = any(
                char in condition for char in '<>'
            )
            where_list.append(condition)
            if '=' not in condition or other_comparisions:
                continue
            alias, field, const = re.split(r'[.=]', condition)
            begin, end = '{', '}'
            equalities[alias] = f'{begin}{field}:{const}{end}'
        if len(equalities) == len(where_list):
            self.aliases.update(equalities)
            self.has_default[WHERE] = True
            return self.LINE_BREAK
        return self.join_with_tabs(where_list, ' AND ') + self.LINE_BREAK

    def set_group(self, values: list) -> str:
        return ''

    def __init__(self, target: 'Select'):
        super().__init__(target)
        self.aliases = {}
        self.KEYWORDS = [WHERE, FROM, ORDER_BY, SELECT]

    def prefix(self, key: str):
        default_prefix = any([
            (key == WHERE and not self.has_default[WHERE]),
            key == ORDER_BY
        ])
        if default_prefix:
            return super().prefix(key)
        return ''


class DataAnalysisLanguage(QueryLanguage):
    def __init__(self, target: 'Select'):
        super().__init__(target)
        self.aggregation_fields = []

    def split_agg_fields(self, values: list) -> list:
        AGG_FUNC_REGEX = re.compile(
            r'({})[(]'.format(
                '|'.join(cls.__name__ for cls in Aggregate.__subclasses__())
            ),
            re.IGNORECASE
        )
        common_fields = []
        for field in values:
            field = self.remove_alias(field)
            if AGG_FUNC_REGEX.findall(field):
                self.aggregation_fields.append(field)
            else:
                common_fields.append(field)
        return common_fields

class DatabricksLanguage(DataAnalysisLanguage):
    pattern = '{_from}{where}{group_by}{order_by}{select}{limit}'
    has_default = {key: bool(key == SELECT) for key in KEYWORD}

    def add_field(self, values: list) -> str:
        return super().add_field(
            self.split_agg_fields(values)
        )

    def prefix(self, key: str) -> str:
        def get_aggregate() -> str:
            return 'AGGREGATE {} '.format(
                ','.join(self.aggregation_fields)
            ) 
        return '{}{}{}{}{}'.format(
            self.LINE_BREAK,
            '|> ' if key != FROM else '',
            get_aggregate() if key == GROUP_BY else '',
            key, self.TABULATION
        )


class FileExtension(Enum):
    CSV = 'read_csv'
    XLSX = 'read_excel'
    JSON = 'read_json'
    HTML = 'read_html'

class PandasLanguage(DataAnalysisLanguage):
    pattern = '{_from}{where}{select}{group_by}{order_by}'
    has_default = {key: False for key in KEYWORD}
    file_extension = FileExtension.CSV
    HEADER_IMPORT_LIB  = ['import pandas as pd']
    LIB_INITIALIZATION = ''
    FIELD_LIST_FMT = '[[{}{}]]'
    PREFIX_LIBRARY = 'pd.'

    def add_field(self, values: list) -> str:
        def line_field_fmt(field: str) -> str:
            return "{}'{}'".format(
                self.TABULATION, field
            )
        common_fields = self.split_agg_fields(values)
        if common_fields:
            return self.FIELD_LIST_FMT.format(
                ','.join(line_field_fmt(fld) for fld in common_fields),
                self.LINE_BREAK
            )
        return ''

    def merge_tables(self, elements: list, main_table: str) -> str:
        a1, f1, a2, f2 = elements
        return "\n\ndf_{} = pd.merge(\n\tdf_{}, df_{}, left_on='{}', right_on='{}', how='{}'\n)\n".format(
            main_table, self.names[a1], self.names[a2], f1, f2, 'inner'
        )

    def get_tables(self, values: list) -> str:
        result = '\n'.join(self.HEADER_IMPORT_LIB) + '\n'
        if self.LIB_INITIALIZATION:
            result += f'\n{self.LIB_INITIALIZATION}'
        self.names = {}
        for table in values:
            table, *join = [t.strip() for t in re.split('JOIN|LEFT|RIGHT|ON', table) if t.strip()]
            alias, table = SQLObject.split_alias(table)
            result += "\ndf_{table} = {prefix}{func}('{table}.{ext}')".format(
                prefix=self.PREFIX_LIBRARY, func=self.file_extension.value,
                table=table, ext=self.file_extension.name.lower()
            )
            self.names[alias] = table
            if join:
                result += self.merge_tables([
                    r.strip() for r in re.split('[().=]', join[-1]) if r
                ], last_table)
            last_table = table
        _, table = SQLObject.split_alias(values[0])
        result += f'\ndf = df_{table}\n\ndf = df'
        return result
    
    def split_condition_elements(self, expr: str) -> list:
        expr = self.remove_alias(expr)
        return [t for t in re.split(r'(\w+)', expr) if t.strip()]

    def extract_conditions(self, values: list) -> str:
        conditions = []
        STR_FUNC = {
            1: '.str.startswith(',
            2: '.str.endswith(',
            3: '.str.contains(',
        }
        for expr in values:
            field, op, *const = self.split_condition_elements(expr)
            if op.upper() == 'LIKE' and len(const) == 3:
                level = 0
                if '%' in const[0]:
                    level += 2
                if '%' in const[2]:
                    level += 1
                const = f"'{const[1]}')"
                op = STR_FUNC[level]
            else:
                const = ''.join(const)
            conditions.append(
                f"(df['{field}']{op}{const})"
            )
        if not conditions:
            return ''
        return '[\n{}\n]'.format(
            '&'.join(f'\t{c}' for c in conditions),
        )
    
    def clean_values(self, values: list) -> str:
        for i in range(len(values)):
            content = self.remove_alias(values[i])
            values[i] = f"'{content}'"
        return ','.join(values)

    def sort_by(self, values: list) -> str:
        if not values:
            return ''
        return '.sort_values(\n{},\n\tascending = {}\n)'.format(
            '\t'+self.clean_values(values), OrderBy.ascending(values[-1])
        )

    def set_group(self, values: list) -> str:
        result = '.groupby([\n\t{}\n])'.format(
            self.clean_values(values)
        )
        if self.aggregation_fields:            
            PANDAS_AGG_FUNC = {'Avg': 'mean', 'Count': 'size'}
            result += '.agg({'
            for field in self.aggregation_fields:
                func, field, *alias = re.split('[()]', field) # [To-Do: Use `alias`]
                result += "{}'{}': ['{}']".format(
                    self.TABULATION, field,
                    PANDAS_AGG_FUNC.get(func, func)
                )
            result += '\n})'
        return result
    
    def __init__(self, target: 'Select'):
        super().__init__(target)
        self.result['function'] = 'find'

    def prefix(self, key: str):
        return ''


class SparkLanguage(PandasLanguage):
    HEADER_IMPORT_LIB = [
        'from pyspark.sql import SparkSession',
        'from pyspark.sql.functions import col, avg, sum, count'
    ]
    FIELD_LIST_FMT = '.select({}{})'
    PREFIX_LIBRARY = 'pyspark.pandas.'

    def merge_tables(self, elements: list, main_table: str) -> str:
        a1, f1, a2, f2 = elements
        COMMAND_FMT = """{cr}
        df_{result} = df_{table1}.join(
            {indent}df_{table2},
            {indent}df_{table1}.{fk_field}{op}df_{table2}.{primary_key}{cr}
        )
        """
        return re.sub(r'\s+', '', COMMAND_FMT).format(
            result=main_table, cr=self.LINE_BREAK, indent=self.TABULATION,
            table1=self.names[a1], table2=self.names[a2],
            fk_field=f1, primary_key=f2, op=' == '
        )

    def extract_conditions(self, values: list) -> str:
        conditions = []
        for expr in values:
            field, op, *const = self.split_condition_elements(expr)
            const = ''.join(const)
            if op.upper() == 'LIKE':
                line = f"\n\t( col('{field}').like({const}) )"
            else:
                line = f"\n\t( col('{field}') {op} {const} )"
            conditions.append(line)
        if not conditions:
            return ''
        return '.filter({}\n)'.format(
            '\n\t&'.join(conditions)
        )

    def sort_by(self, values: list) -> str:
        if not values:
            return ''
        return '.orderBy({}{}{})'.format(
            self.TABULATION,
            self.clean_values(values),
            self.LINE_BREAK
        )

    def set_group(self, values: list) -> str:
        result = '.groupBy({}{}{})'.format(
            self.TABULATION,
            self.clean_values(values),
            self.LINE_BREAK
        )
        if self.aggregation_fields:            
            result += '.agg('
            for field in self.aggregation_fields:
                func, field, *alias = re.split(r'[()]|\s+as\s+|\s+AS\s+', field)
                result += "{}{}('{}')".format(
                    self.TABULATION, func.lower(), 
                    field if field else '*'
                )
                if alias:
                    result += f".alias('{alias[-1]}')"
            result += '\n)'
        return result


class Parser:
    REGEX = {}

    def prepare(self):
        ...

    def __init__(self, txt: str, class_type):
        self.queries = []
        self.prepare()
        self.class_type = class_type
        self.eval(txt)

    def eval(self, txt: str):
        ...

    @staticmethod
    def remove_spaces(script: str) -> str:
        is_string = False
        result = []
        for token in re.split(r'(")', script):
            if token == '"':
                is_string = not is_string
            if not is_string:
                token = re.sub(r'\s+', '', token)
            result.append(token)
        return ''.join(result)

    def get_tokens(self, txt: str) -> list:
        return [
            self.remove_spaces(t)
            for t in self.REGEX['separator'].split(txt)            
        ]


class JoinType(Enum):
    INNER = ''
    LEFT = 'LEFT '
    RIGHT = 'RIGHT '
    FULL = 'FULL '


class SQLParser(Parser):
    REGEX = {}

    def prepare(self):
        keywords = '|'.join(k + r'\b' for k in KEYWORD)
        flags = re.IGNORECASE + re.MULTILINE
        self.REGEX['keywords'] = re.compile(f'({keywords})', flags)
        self.REGEX['subquery'] = re.compile(r'(\w\.)*\w+ +in +\(SELECT.*?\)', flags)

    def eval(self, txt: str):
        def find_last_word(pos: int) -> int:
            SPACE, WORD = 1, 2
            found = set()
            for i in range(pos, 0, -1):
                if txt[i] in [' ', '\t', '\n']:
                    if sum(found) == 3:
                        return i
                    found.add(SPACE)
                if txt[i].isalpha():
                    found.add(WORD)
                elif txt[i] == '.':
                    found.remove(WORD)
        def find_parenthesis(pos: int) -> int:
            for i in range(pos, len(txt)-1):
                if txt[i] == ')':
                    return i+1
        result = {}
        found = self.REGEX['subquery'].search(txt)
        while found:
            start, end = found.span()
            inner = txt[start: end]
            if inner.count('(') > inner.count(')'):
                end = find_parenthesis(end)
                inner = txt[start: end-1]
            fld, *inner = re.split(r' IN | in', inner, maxsplit=1)
            if fld.upper() == 'NOT':
                pos = find_last_word(start)
                fld = txt[pos: start].strip() # [To-Do] Use the value of `fld`
                start = pos
                target_class = NotSelectIN
            else:
                target_class = SelectIN
            obj = SQLParser(
                ' '.join(re.sub(r'^\(', '', s.strip()) for s in inner),
                class_type=target_class
            ).queries[0]
            result[obj.alias] = obj
            txt = txt[:start-1] + txt[end+1:]
            found = self.REGEX['subquery'].search(txt)
        tokens = [t.strip() for t in self.REGEX['keywords'].split(txt) if t.strip()]
        values = {k.upper(): v for k, v in zip(tokens[::2], tokens[1::2])}
        tables = [t.strip() for t in re.split('JOIN|LEFT|RIGHT|ON', values[FROM]) if t.strip()]
        for item in tables:
            if '=' in item:
                a1, f1, a2, f2 = [r.strip() for r in re.split('[().=]', item) if r]
                obj1: SQLObject = result[a1]
                obj2: SQLObject = result[a2]
                PrimaryKey.add(f2, obj2)
                ForeignKey(obj2.table_name).add(f1, obj1)
            else:
                obj = self.class_type(item)
                for key in USUAL_KEYS:
                    if not key in values:
                        continue
                    cls = {
                        ORDER_BY: OrderBy, GROUP_BY: GroupBy
                    }.get(key, Field)
                    obj.values[key] = [
                        cls.format(fld, obj)
                        for fld in self.class_type.split_fields(values[key], key)
                        if (fld != '*' and len(tables) == 1) or obj.match(fld, key)
                    ]
                result[obj.alias] = obj
        self.queries = list( result.values() )


class CypherParser(Parser):
    REGEX = {}
    CHAR_SET = r'[(,?)^{}[\]]'
    KEYWORDS = '|'.join(
        fr'\b{word}\b'
        for word in "where return WHERE RETURN and AND".split()
    )

    def prepare(self):
        self.REGEX['separator'] = re.compile(fr'({self.CHAR_SET}|->|<-|{self.KEYWORDS})')
        self.REGEX['condition'] = re.compile(r'(^\w+)|([<>=])')
        self.REGEX['alias_pos'] = re.compile(r'(\w+)[.](\w+)')
        self.join_type = JoinType.INNER
        self.TOKEN_METHODS = {
            '(': self.add_field,  '?': self.add_where,
            ',': self.add_field,  '^': self.add_order,
            ')': self.new_query,  '<-': self.left_ftable,
            '->': self.right_ftable,
        }
        self.method = self.new_query
        self.aliases = {}

    def new_query(self, token: str, join_type = JoinType.INNER, alias: str=''):
        token, *group_fields = token.split('@')
        if not token.isidentifier():
            return
        table_name = f'{token} {alias}' if alias else token
        query = self.class_type(table_name)
        if not alias:
            alias = query.alias
        self.queries.append(query)
        self.aliases[alias] = query
        FieldList(group_fields, [Field, GroupBy]).add('', query)
        query.join_type = join_type

    def add_where(self, token: str):
        elements = [t for t in self.REGEX['alias_pos'].split(token) if t]
        if len(elements) == 3:
            alias, field, *condition = elements
            query = self.aliases[alias]
        else:
            field, *condition = [
                t for t in self.REGEX['condition'].split(token) if t
            ]
            query = self.queries[-1]
        Where(' '.join(condition)).add(field, query)
    
    def add_order(self, token: str):
        self.add_field(token, sorted=True)

    def add_field(self, token: str, sorted: bool = False):
        if token in self.TOKEN_METHODS:
            return
        if '*' in token:
            pk_field = token.replace('*', '')
            if not pk_field.isidentifier():
                pos = int(pk_field or '1')-1
                pk_field = self.queries[-1].values[SELECT][pos]
            self.queries[-1].key_field = pk_field.split('.')[-1]
            return
        # -------------------------------------------------------
        def field_params() -> dict:
            ROLE_OF_SEPARATOR = {
                '$': 'function',
                ':': 'alias',
                '@': 'group',
                '!': 'field',
            }
            REGEX_FIELD = r'([{}])'.format(''.join(ROLE_OF_SEPARATOR))
            elements = re.split(REGEX_FIELD, token+'!')
            return {
                ROLE_OF_SEPARATOR[k]: v 
                for k, v in zip(elements[1::2], elements[::2])
            }
        def run(function: str='', alias: str='', group: str='', field: str=''):
            is_count = function == 'count'
            if alias or is_count:
                field, alias = alias, field
            extra_classes = [OrderBy] if sorted else []
            if group:
                if not field:
                    field = group
                elif not alias:
                    alias = group
                extra_classes += [GroupBy]
            if function:                
                if is_count and not field:
                    field = self.queries[-1].key_field or 'id'
                class_type = FUNCTION_CLASS.get(function)
                if not class_type:
                    raise ValueError(f'Unknown function `{function}`.')
                class_list = [ class_type().As(alias, extra_classes) ]
            elif alias:
                class_list = [NamedField(alias)] + extra_classes
            else:
                class_list = [Field] + extra_classes
            FieldList(field, class_list).add('', self.queries[-1])
        # -------------------------------------------------------
        run( **field_params() )
        # -------------------------------------------------------

    def left_ftable(self, token: str):
        if self.queries:
            self.queries[-1].join_type = JoinType.LEFT
        self.new_query(token)

    def right_ftable(self, token: str):
        self.new_query(token, JoinType.RIGHT)

    def add_foreign_key(self, token: str, pk_field: str=''):
        curr, last = [self.queries[i] for i in (-1, -2)]
        if not pk_field:
            if last.key_field:
                pk_field = last.key_field
            else:
                if not last.values.get(SELECT):
                    raise IndexError(f'Primary Key not found for {last.table_name}.')
                pk_field = last.values[SELECT][-1].split('.')[-1]
                last.delete(pk_field, [SELECT], exact=True)
        if '{}' in token:
            foreign_fld = token.format(
                last.table_name.lower()
                if last.join_type == JoinType.LEFT else
                curr.table_name.lower()
            )
        else:
            if not curr.values.get(SELECT):
                raise IndexError(f'Foreign Key not found for {curr.table_name}.')
            fields = [
                fld for fld in curr.values[SELECT]
                if fld not in curr.values.get(GROUP_BY, [])
            ]
            foreign_fld = fields[0].split('.')[-1]
            curr.delete(foreign_fld, [SELECT], exact=True)
            if curr.join_type == JoinType.RIGHT:
                pk_field, foreign_fld = foreign_fld, pk_field
        if curr.join_type == JoinType.RIGHT:
            curr, last = last, curr
        k = ForeignKey.get_key(curr, last)
        ForeignKey.references[k] = (foreign_fld, pk_field)

    def fk_charset(self) -> str:
        return '(['

    def eval(self, txt: str):
        # ====================================
        def has_side_table() -> bool:
            count = 0 if len(self.queries) < 2 else sum(
                q.join_type != JoinType.INNER
                for q in self.queries[-2:]
            )
            return count > 0
        # -----------------------------------
        for token in self.get_tokens(txt):
            if not token or (token in '([' and self.method):
                continue
            if self.method:
                self.method(token)
            if token in ')]' and has_side_table():
                self.add_foreign_key('')
            self.method = self.TOKEN_METHODS.get(token.upper())
        # ====================================

class Neo4JParser(CypherParser):
    def prepare(self):
        super().prepare()
        self.TOKEN_METHODS = {
            '(': self.new_query,  '{': self.add_where, '[': self.new_query,
            '<-': self.left_ftable, '->': self.right_ftable,            
            'WHERE': self.add_where, 'AND': self.add_where, 
        }
        self.method = None
        self.aliases = {}

    def new_query(self, token: str, join_type = JoinType.INNER):
        alias = ''
        if ':' in token:
            alias, token = token.split(':')
        super().new_query(token, join_type, alias)

    def add_where(self, token: str):
        super().add_where(token.replace(':', '='))

    def add_foreign_key(self, token: str, pk_field: str='') -> tuple:
        return super().add_foreign_key('{}_id', 'id')

# ----------------------------
class MongoParser(Parser):
    REGEX = {}

    def prepare(self):
        self.REGEX['separator'] = re.compile(r'([({[\]},)])')

    def new_query(self, token: str):
        if not token:
            return
        *table, function = token.split('.')
        self.param_type = self.PARAM_BY_FUNCTION.get(function)
        if not self.param_type:            
            raise SyntaxError(f'Unknown function {function}')
        if table and table[0]:
            self.queries.append( self.class_type(table[-1]) )

    def param_is_where(self) -> bool:
        return self.param_type == Where or isinstance(self.param_type, Where)

    def next_param(self, token: str):
        if self.param_type == GroupBy:
            self.param_type = Field
        self.get_param(token)

    def get_param(self, token: str):
        if not ':' in token:
            return
        field, value = token.split(':')
        is_function = field.startswith('$')
        if not value and not is_function:
            if self.param_is_where():
                self.last_field = field
            return
        if self.param_is_where():
            if is_function:
                function = field
                field = self.last_field
                self.last_field = ''
            else:
                function = '$eq'
            if '"' in value:
                value = value.replace('"', '')
            elif value and value[0].isnumeric():
                numeric_type = float if len(value.split('.')) == 2 else int
                value = numeric_type(value)
            self.param_type = self.CONDITIONS[function](value)
            if function == '$or':
                return
        elif self.param_type == GroupBy:
            if field != '_id':
                return
            field = re.sub('"|[$]', '', value)
        elif self.param_type == OrderBy and value == '-1':
            OrderBy.sort = SortType.DESC
        elif field.startswith('$'):
            field = '{}({})'.format(
                field.replace('$', ''), value
            )
        if self.where_list is not None and self.param_is_where():
            self.where_list[field] = self.param_type
            return
        self.param_type.add(field, self.queries[-1])

    def close_brackets(self, token: str):
        self.brackets[token] -= 1
        if self.param_is_where() and self.brackets[token] == 0:
            if self.where_list is not None:
                Options(**self.where_list).add('OR', self.queries[-1])
                self.where_list = None
            if token == '{':
                self.param_type = Field

    def begin_conditions(self, value: str):
        self.where_list = {}
        self.field_method = self.first_ORfield
        return Where
    
    def first_ORfield(self, text: str):
        if text.startswith('$'):
            return
        found = re.search(r'\w+[:]', text)
        if not found:
            return
        self.field_method = None
        p1, p2 = found.span()
        self.last_field = text[p1: p2-1]

    def increment_brackets(self, value: str):
        self.brackets[value] += 1

    def eval(self, txt: str):
        self.method = self.new_query
        self.last_field = ''
        self.where_list = None
        self.field_method = None
        self.PARAM_BY_FUNCTION = {
            'find': Where, 'aggregate': GroupBy, 'sort': OrderBy
        }
        BRACKET_PAIR = {'}': '{', ']': '['}
        self.brackets = {char: 0 for char in BRACKET_PAIR.values()}
        self.CONDITIONS = {
            '$in': lambda value: contains(value),
            '$gt': lambda value: gt(value),
            '$gte' : lambda value: gte(value),
            '$lt': lambda value: lt(value),
            '$lte' : lambda value: lte(value),
            '$eq': lambda value: eq(value),
            '$ne': lambda value: Not.eq(value),
            '$or': self.begin_conditions,
        }
        self.TOKEN_METHODS = {
            '{': self.get_param, ',': self.next_param, ')': self.new_query, 
        }
        for token in self.get_tokens(txt):
            if not token:
                continue
            if self.method:
                self.method(token)
            if token in self.brackets:
                self.increment_brackets(token)
            elif token in BRACKET_PAIR:
                self.close_brackets(
                    BRACKET_PAIR[token]
                )
            elif self.field_method:
                self.field_method(token)
            self.method = self.TOKEN_METHODS.get(token)
# ----------------------------


class Select(SQLObject):
    join_type: JoinType = JoinType.INNER
    EQUIVALENT_NAMES = {}
    DefaultLanguage = QueryLanguage

    def __init__(self, table_name: str='', **values):
        super().__init__(table_name)
        self.__call__(**values)
        self.break_lines = True

    def update_values(self, key: str, new_values: list):
        for value in self.diff(key, new_values):
            self.values.setdefault(key, []).append(value)

    def aka(self) -> str:
        result = self.table_name
        return self.EQUIVALENT_NAMES.get(result, result)

    def add(self, name: str, main: SQLObject):
        old_tables = main.values.get(FROM, [])
        if len(self.values[FROM]) > 1:
            old_tables += self.values[FROM][1:]
        new_tables = []
        row = '{jt}JOIN {tb} {a2} ON ({a1}.{f1} = {a2}.{f2})'.format(
                jt=self.join_type.value,
                tb=self.aka(),
                a1=main.alias, f1=name,
                a2=self.alias, f2=self.key_field
            )
        if row not in old_tables[1:]:
            new_tables.append(row)
        main.values[FROM] = old_tables[:1] + new_tables + old_tables[1:]
        for key in USUAL_KEYS:
            main.update_values(key, self.values.get(key, []))

    def copy(self) -> SQLObject:
        from copy import deepcopy
        return deepcopy(self)

    def no_relation_error(self, other: SQLObject):
        raise ValueError(f'No relationship found between {self.table_name} and {other.table_name}.')

    def __add__(self, other: SQLObject):
        query = self.copy()
        if query.table_name.lower() == other.table_name.lower():
            for key in USUAL_KEYS:
                query.update_values(key, other.values.get(key, []))
            return query
        foreign_field, primary_key = ForeignKey.find(query, other)
        if not foreign_field:
            foreign_field, primary_key = ForeignKey.find(other, query)
            if foreign_field:
                if primary_key:
                    PrimaryKey.add(primary_key, query)
                query.add(foreign_field, other)
                return other
            self.no_relation_error(other) # === raise ERROR ...  ===
        elif primary_key:
            PrimaryKey.add(primary_key, other)
        other.add(foreign_field, query)
        return query

    def __str__(self) -> str:
        return self.translate_to(self.DefaultLanguage)
   
    def __call__(self, **values):
        for name, params in values.items():
            for obj in TO_LIST(params):
                obj.add(name, self)
        return self

    def __eq__(self, other: SQLObject) -> bool:
        for key in KEYWORD:
            if self.diff(key, other.values.get(key, []), True):
                return False
        return True
    
    def __sub__(self, other: SQLObject) -> SQLObject:        
        fk_field, primary_k = ForeignKey.find(self, other)
        if fk_field:
            query = self.copy()
            other = other.copy()
        else:
            fk_field, primary_k = ForeignKey.find(other, self)
            if not fk_field:
                self.no_relation_error(other) # === raise ERROR ...  ===
            query = other.copy()
            other = self.copy()
        query.__class__ = NotSelectIN
        Field.add(fk_field, query)
        query.add(primary_k, other)
        return other

    def limit(self, row_count: int=100, offset: int=0):
        if Function.dialect == Dialect.SQL_SERVER:
            fields = self.values.get(SELECT)
            if fields:
                fields[0] = f'SELECT TOP({row_count}) {fields[0]}'
            else:
                self.values[SELECT] = [f'SELECT TOP({row_count}) *']
            return self
        if Function.dialect == Dialect.ORACLE:
            Where.gte(row_count).add(SQL_ROW_NUM, self)
            if offset > 0:
                Where.lte(row_count+offset).add(SQL_ROW_NUM, self)
            return self
        self.values[LIMIT] = ['{}{}'.format(
            row_count, f' OFFSET {offset}' if offset > 0 else ''
        )]
        return self

    def match(self, field: str, key: str) -> bool:
        '''
        Recognizes if the field is from the current table
        '''
        if key in (ORDER_BY, GROUP_BY) and '.' not in field:
            return self.has_named_field(field)
        return re.findall(f'\b*{self.alias}[.]', field) != []

    @classmethod
    def parse(cls, txt: str, parser: Parser = SQLParser) -> list[SQLObject]:
        return parser(txt, cls).queries

    def optimize(self, rules: list[Rule]=None):
        if not rules:
            rules = Rule.__subclasses__()
        for rule in rules:
            rule.apply(self)

    def add_fields(self, fields: list, class_types=None):
        if not class_types:
            class_types = []
        class_types += [Field]
        FieldList(fields, class_types).add('', self)

    def translate_to(self, language: QueryLanguage) -> str:
        return language(self).convert()


class SelectIN(Select):
    condition_class = Where

    def add(self, name: str, main: SQLObject):
        self.break_lines = False
        self.condition_class.inside(self).add(name, main)

SubSelect = SelectIN

class NotSelectIN(SelectIN):
    condition_class = Not


class CTE(Select):
    prefix = ''
    show_query = True

    def __init__(self, table_name: str, query_list: list[Select]=[]):
        super().__init__(table_name)
        self.query_list = query_list
        self.break_lines = False        

    def __str__(self) -> str:
        size = 0
        for key in USUAL_KEYS:
            size += sum(len(v) for v in self.values.get(key, []) if '\n' not in v)
        if size > 70:
            self.break_lines = True
        # ---------------------------------------------------------
        def justify(query: Select) -> str:
            query.break_lines = False
            result, line = [], ''
            keywords = '|'.join(KEYWORD)
            for word in re.split(fr'({keywords}|AND|OR|,)', str(query)):
                if len(line) >= 50:
                    result.append(line)
                    line = ''
                line += word
            if line:
                result.append(line)
            return '\n    '.join(result)
        # ---------------------------------------------------------
        return 'WITH {}{} AS (\n    {}\n){}'.format(
            self.prefix, self.table_name, 
            '\n\tUNION ALL\n    '.join(
                justify(q) for q in self.query_list
            ), super().__str__() if self.show_query else ''
        )

    def join(self, pattern: str, fields: list | str, format: str=''):
        if isinstance(fields, str):
            count = len( fields.split(',') )
        else:
            count = len(fields)
        queries = detect(
            pattern*count, join_method=None, format=format
        )
        FieldList(fields, queries, ziped=True).add('', self)
        self.break_lines = True
        return self

class Recursive(CTE):
    prefix = 'RECURSIVE '

    def __str__(self) -> str:
        if len(self.query_list) > 1:
            self.query_list[-1].values[FROM].append(
                f', {self.table_name} {self.alias}')
        return super().__str__()

    @classmethod
    def create(cls, name: str, pattern: str, formula: str, init_value, format: str=''):
        SQLObject.ALIAS_FUNC = None
        def get_field(obj: SQLObject, pos: int) -> str:
            return obj.values[SELECT][pos].split('.')[-1]
        t1, t2 = detect(
            pattern*2, join_method=None, format=format
        )
        pk_field = get_field(t1, 0)
        foreign_key = ''
        for num in re.findall(r'\[(\d+)\]', formula):
            num = int(num)
            if not foreign_key:
                foreign_key = get_field(t2, num-1)
                formula = formula.replace(f'[{num}]', '%')
            else:
                formula = formula.replace(f'[{num}]', get_field(t2, num-1))
        Where.eq(init_value).add(pk_field, t1)
        Where.formula(formula).add(foreign_key or pk_field, t2)
        return cls(name, [t1, t2])

    def counter(self, name: str, start, increment: str='+1'):
        for i, query in enumerate(self.query_list):
            if i == 0:
                Field.add(f'{start} AS {name}', query)
            else:
                Field.add(f'({name}{increment}) AS {name}', query)
        return self


MAIN_TAG = '__main__'

class CTEFactory:
    def __init__(self, txt: str):
        """
        SQL syntax:
        ---
        **SELECT field, field
        FROM** ( `sub_query1` ) **AS** `alias_1`
        JOIN ( `sub_query2` ) **AS** `alias_2` **ON** `__join__`
        
        Cypher syntax:
        ---
        Table1(field, `function$`field`:alias`, `group@`) <- Table2(field)
        `...`MainTable(field)
        """
        self.main = None
        if parser_class(txt) == CypherParser:
            if '...' in txt:
                txt, other = txt.split('...')
                self.main = detect(other)
                alias = self.main.table_name
            query_list = Select.parse(txt, CypherParser)
            if not self.main:
                alias = '_'.join(query.table_name for query in query_list)
                self.main = Select(alias)
                self.main.break_lines = False
            query = join_queries(query_list)
            self.cte_list = [CTE(alias, [query])]
            return 
        summary = self.extract_subqueries(txt)
        self.main = detect( summary.pop(MAIN_TAG) )
        self.cte_list = [
            CTE(alias, [
                Select.parse(query)[0]
                for query in elements
            ])
            for alias, elements in summary.items()
        ]

    def __str__(self):
        CTE.show_query = False
        lines = [str(cte) for cte in self.cte_list]
        result = ',\n'.join(lines) + '\n' + str(self.main)
        CTE.show_query = True
        return result    

    @staticmethod
    def extract_subqueries(txt: str) -> dict:        
        result = {}
        # ---------------------------------------------------
        def clean_subquery(source: list) -> str:
            while source:
                if source[0].upper() == 'SELECT':
                    break
                word = source.pop(0)
                if word.upper() in ('FROM', 'JOIN'):
                    result[MAIN_TAG] += f' {word}'
            return ' '.join(source)
        def balanced_parentheses(expr: str) -> bool:
            return expr.count('(') == expr.count(')')
        # ---------------------------------------------------
        for found in re.finditer(r'(FROM|JOIN)\s*[(]\s*SELECT', txt, re.IGNORECASE):
            start = found.start()
            alias = ''
            pos = start
            while not alias:
                found = re.search(r'[)]\s*AS\s+\w+', txt[pos:], re.IGNORECASE)
                if not found:
                    break
                end = found.end() + pos
                last = end
                if balanced_parentheses(txt[start: end]):
                    pos += found.start()
                    alias = re.findall(r'\s*(\w+)$', txt[pos: end])[0]
                    end = pos
                pos = end
            if not result:
                result[MAIN_TAG] = txt[:start]
            query_list = [
                clean_subquery( expr.split() )
                for expr in  re.split(
                    r'\bUNION\b', txt[start: end], re.IGNORECASE
                )
            ]
            result[MAIN_TAG] += f' {alias} {alias}'
            result[alias] = query_list
        result[MAIN_TAG] += txt[last:]
        return result


# ----- Rules -----

class RulePutLimit(Rule):
    @classmethod
    def apply(cls, target: Select):
        need_limit = any(not target.values.get(key) for key in (WHERE, SELECT))
        if need_limit:
            target.limit()


class RuleSelectIN(Rule):
    @classmethod
    def apply(cls, target: Select):
        for i, condition in enumerate(target.values[WHERE]):
            tokens = re.split(r'\s+or\s+|\s+OR\s+', re.sub('\n|\t|[()]', ' ', condition))
            if len(tokens) < 2:
                continue
            fields = [t.split('=')[0].split('.')[-1].lower().strip() for t in tokens]
            if len(set(fields)) == 1:
                target.values[WHERE][i] = '{} IN ({})'.format(
                    Field.format(fields[0], target),
                    ','.join(t.split('=')[-1].strip() for t in tokens)
                )


class RuleAutoField(Rule):
    @classmethod
    def apply(cls, target: Select):
        if target.values.get(GROUP_BY):
            target.values[SELECT] = target.values[GROUP_BY]
            target.values[ORDER_BY] = []
        elif target.values.get(ORDER_BY):
            s1 = set(target.values.get(SELECT, []))
            s2 = set(target.values[ORDER_BY])
            target.values.setdefault(SELECT, []).extend( list(s2-s1) )


class RuleLogicalOp(Rule):
    REVERSE = {">=": "<", "<=": ">", "=": "<>"}
    REVERSE |= {v: k for k, v in REVERSE.items()}

    @classmethod
    def apply(cls, target: Select):
        REGEX = re.compile('({})'.format(
            '|'.join(cls.REVERSE)
        ))
        for i, condition in enumerate(target.values.get(WHERE, [])):
            expr = re.sub('\n|\t', ' ', condition)
            if not re.search(r'\b(NOT|not).*[<>=]', expr):
                continue
            tokens = [t.strip() for t in re.split(r'NOT\b|not\b|(<|>|=)', expr) if t]
            op = ''.join(tokens[1: len(tokens)-1])
            tokens = [tokens[0], cls.REVERSE[op], tokens[-1]]
            target.values[WHERE][i] = ' '.join(tokens)


class RuleDateFuncReplace(Rule):
    """
    SQL algorithm by Ralff Matias
    """
    REGEX = re.compile(r'(YEAR[(]|year[(]|=|[)])')

    @classmethod
    def apply(cls, target: Select):
        for i, condition in enumerate(target.values.get(WHERE, [])):
            if not '(' in condition:
                continue
            tokens = [
                t.strip() for t in cls.REGEX.split(condition) if t.strip()
            ]
            if len(tokens) < 3:
                continue
            func, field, *rest, year = tokens
            temp = Select(f'{target.table_name} {target.alias}')
            Between(f'{year}-01-01', f'{year}-12-31').add(field, temp)
            target.values[WHERE][i] = ' AND '.join(temp.values[WHERE])


class RuleReplaceJoinBySubselect(Rule):
    @classmethod
    def apply(cls, target: Select):
        main, *others = Select.parse( str(target) )
        modified = False
        for query in others:
            fk_field, primary_k = ForeignKey.find(main, query)
            more_relations = any([
                ref[0] == query.table_name for ref in ForeignKey.references
            ])
            keep_join = any([
                len( query.values.get(SELECT, []) ) > 0,
                len( query.values.get(WHERE, []) ) == 0,
                not fk_field, more_relations
            ])
            if keep_join:
                query.add(fk_field, main)
                continue
            query.__class__ = SubSelect
            Field.add(primary_k, query)
            query.add(fk_field, main)
            modified = True
        if modified:
            target.values = main.values.copy()


def parser_class(text: str) -> Parser:
    PARSER_REGEX = [
        (r'select.*from', SQLParser),
        (r'[.](find|aggregate)[(]', MongoParser),
        (r'[(\[]\w*[:]\w+', Neo4JParser),
        (r'^\w+[@]*\w*[(]', CypherParser)
    ]
    text = Parser.remove_spaces(text)
    for regex, class_type in PARSER_REGEX:
        if re.findall(regex, text, re.IGNORECASE):
            return class_type
    return None

def join_queries(query_list: list) -> Select:
    result = query_list[0]
    for query in query_list[1:]:
        result += query
    return result

def detect(text: str, join_method = join_queries, format: str='') -> Select | list[Select]:
    from collections import Counter
    parser = parser_class(text)
    if not parser:
        raise SyntaxError('Unknown parser class')
    if parser == CypherParser:
        for table, count in Counter( re.findall(r'(\w+)[(]', text) ).most_common():
            if count < 2:
                continue
            pos = [ f.span() for f in re.finditer(fr'({table})[(]', text) ]
            for begin, end in pos[::-1]:
                new_name = f'{table}_{count}'  # See set_table (line 55)
                Select.EQUIVALENT_NAMES[new_name] = table
                text = text[:begin] + new_name + '(' + text[end:]
                count -= 1
    result = Select.parse(text, parser)
    if format:
        for query in result:
            query.set_file_format(format)
    if join_method:
        result = join_method(result)
    return result
# ===========================================================================================//

if __name__ == "__main__":
    cte = CTEFactory(
        "Sales(year$ref_date:ref_year@, sum$quantity:qty_sold, vendor) <- Vendor(id, name:vendors_name@)"
        # ^^^   ^^^           ^^^
        #  |     |             |                                       ^^^            ^^^
        #  |     |             |                                        |              |
        #  |     |             |         Relate Sales to Vendor --------+              |
        #  |     |             |                                                       |
        #  |     |             +---- Call it `ref_year` and group it                   |
        #  |     |                                                                     |
        #  |     +-- Extracts the year from the `ref_date` field                       |
        #  |                                                                           |
        #  +--- The Sales table                                                        |
        #                               Also groups by vendor´s name ------------------+ 
        "...Annual_Sales_per_Vendor(ref_year, qty_sold, vendors_name, *) -> Goal(year, target)"
    )
    print(cte)
