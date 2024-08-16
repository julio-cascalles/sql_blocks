from enum import Enum
import re


PATTERN_PREFIX = '([^0-9 ]+[.])'
PATTERN_SUFFIX = '( [A-Za-z_]+)'
SUFFIX_AND_PRE = f'{PATTERN_SUFFIX}|{PATTERN_PREFIX}'
DISTINCT_SF_PR = f'(DISTINCT|distinct)|{SUFFIX_AND_PRE}'

KEYWORD = {
    'SELECT': (',{}', 'SELECT *', DISTINCT_SF_PR),
    'FROM': ('{}', '', PATTERN_SUFFIX),
    'WHERE': ('{}AND ', '', f'{PATTERN_PREFIX}| '),
    'GROUP BY': (',{}', '', SUFFIX_AND_PRE),
    'ORDER BY': (',{}', '', SUFFIX_AND_PRE),
    'LIMIT': (' ', '', ''),
}
#              ^    ^        ^
#              |    |        |
#              |    |        +----- pattern to compare fields
#              |    |
#              |    +----- default when empty (SELECT * ...)
#              |
#              +-------- separator

SELECT, FROM, WHERE, GROUP_BY, ORDER_BY, LIMIT = KEYWORD.keys()
USUAL_KEYS = [SELECT, WHERE, GROUP_BY, ORDER_BY, LIMIT]


class SQLObject:
    ALIAS_FUNC = lambda t: t.lower()[:3]
    """    ^^^^^^^^^^^^^^^^^^^^^^^^
    You can change the behavior by assigning 
    a user function to SQLObject.ALIAS_FUNC
    """

    def __init__(self, table_name: str=''):
        self.__alias = ''
        self.values = {}
        self.key_field = ''
        self.set_table(table_name)

    def set_table(self, table_name: str):
        if not table_name:
            return
        if ' ' in table_name.strip():
            table_name, self.__alias = table_name.split()
        elif '_' in table_name:
            self.__alias = ''.join(
                word[0].lower()
                for word in table_name.split('_')
            )
        else:
            self.__alias = SQLObject.ALIAS_FUNC(table_name)
        self.values.setdefault(FROM, []).append(f'{table_name} {self.alias}')

    @property
    def table_name(self) -> str:
        return self.values[FROM][0].split()[0]
    
    @property
    def alias(self) -> str:
        if self.__alias:
            return self.__alias
        return self.table_name
 
    @staticmethod
    def get_separator(key: str) -> str:
        appendix = {WHERE: 'and|', FROM: 'join|JOIN'}
        return KEYWORD[key][0].format(appendix.get(key, ''))

    def diff(self, key: str, search_list: list, symmetrical: bool=False) -> set:
        pattern = KEYWORD[key][2]
        separator = self.get_separator(key)
        def field_set(source: list) -> set:
            return set(
                re.sub(pattern, '', fld.strip()).lower()
                for string in source
                for fld in re.split(separator, string)
            )       
        s1 = field_set(search_list)
        s2 = field_set(self.values.get(key, []))
        if symmetrical:
            return s1.symmetric_difference(s2)
        return s1 - s2

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
        name = name.strip()
        if name in ('_', '*'):
            name = '*'
        elif not re.findall('[.()0-9]', name):
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

    @staticmethod
    def get_key(obj1: SQLObject, obj2: SQLObject) -> tuple:
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
        value = f"'{value}'"
    return str(value)


class Where:
    prefix = ''

    def __init__(self, expr: str):
        self.expr = expr

    @classmethod
    def __constructor(cls, operator: str, value):
        return cls(expr=f'{operator} {quoted(value)}')

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
    
    @classmethod
    def list(cls, values):
        if isinstance(values, list):
            values = ','.join(quoted(v) for v in values)
        return cls(f'IN ({values})')

    def add(self, name: str, main: SQLObject):
        main.values.setdefault(WHERE, []).append('{}{} {}'.format(
            self.prefix, Field.format(name, main), self.expr
        ))


class Not(Where):
    prefix = 'NOT '

    @classmethod
    def eq(cls, value):
        return Where.__constructor('<>', value)


class Case:
    def __init__(self, field: str):
        self.__conditions = {}
        self.default = None
        self.field = field

    def when(self, condition: Where, result: str):
        self.__conditions[result] = condition
        return self
    
    def else_value(self, default: str):
        self.default = default
        return self
    
    def add(self, name: str, main: SQLObject):
        field = Field.format(self.field, main)
        default = quoted(self.default)
        name = 'CASE \n{}\n\tEND AS {}'.format(
            '\n'.join(
                f'\t\tWHEN {field} {cond.expr} THEN {quoted(res)}'
                for res, cond in self.__conditions.items()
            ) + f'\n\t\tELSE {default}' if default else '',
            name
        )
        main.values.setdefault(SELECT, []).append(name)


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


class Rule:
    @classmethod
    def apply(cls, target: 'Select'):
        ...


class JoinType(Enum):
    INNER = ''
    LEFT = 'LEFT '
    RIGHT = 'RIGHT '
    FULL = 'FULL '

class Select(SQLObject):
    join_type: JoinType = JoinType.INNER
    REGEX = {}

    def __init__(self, table_name: str='', **values):
        super().__init__(table_name)
        self.__call__(**values)
        self.break_lines = True

    def update_values(self, key: str, new_values: list):
        for value in self.diff(key, new_values):
            self.values.setdefault(key, []).append(value)

    def add(self, name: str, main: SQLObject):
        main.update_values(
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
            main.update_values(key, self.values.get(key, []))

    def __add__(self, other: SQLObject):
        if self.table_name.lower() == other.table_name.lower():
            for key in USUAL_KEYS:
                self.update_values(key, other.values.get(key, []))
            return self
        foreign_field, primary_key = ForeignKey.find(self, other)
        if not foreign_field:
            foreign_field, primary_key = ForeignKey.find(other, self)
            if foreign_field:
                if primary_key:
                    PrimaryKey.add(primary_key, self)
                self.add(foreign_field, other)
                return other
            raise ValueError(f'No relationship found between {self.table_name} and {other.table_name}.')
        elif primary_key:
            PrimaryKey.add(primary_key, other)
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
        to_list = lambda x: x if isinstance(x, list) else [x]
        for name, params in values.items():
            for obj in to_list(params):
                obj.add(name, self)
        return self

    def __eq__(self, other: SQLObject) -> bool:
        for key in KEYWORD:
            if self.diff(key, other.values.get(key, []), True):
                return False
        return True

    def limit(self, row_count: int=100, offset: int=0):
        result = [str(row_count)]
        if offset > 0:
            result.append(f'OFFSET {offset}')
        self.values.setdefault(LIMIT, result)
        return self

    def match(self, expr: str) -> bool:
        return re.findall(f'\b*{self.alias}[.]', expr) != []

    @classmethod
    def parse(cls, txt: str) -> list[SQLObject]:
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
        if not cls.REGEX:
            keywords = '|'.join(k + r'\b' for k in KEYWORD)
            flags = re.IGNORECASE + re.MULTILINE
            cls.REGEX['keywords'] = re.compile(f'({keywords}|[*])', flags)
            cls.REGEX['subquery'] = re.compile(r'(\w\.)*\w+ +in +\(SELECT.*?\)', flags)
        result = {}
        found = cls.REGEX['subquery'].search(txt)
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
                class_type = NotSelectIN
            else:
                class_type = SelectIN
            obj = class_type.parse(
                ' '.join(re.sub(r'^\(', '', s.strip()) for s in inner)
            )[0]
            result[obj.alias] = obj
            txt = txt[:start-1] + txt[end+1:]
            found = cls.REGEX['subquery'].search(txt)
        tokens = [t.strip() for t in cls.REGEX['keywords'].split(txt) if t.strip()]
        values = {k.upper(): v for k, v in zip(tokens[::2], tokens[1::2])}
        tables = [t.strip() for t in re.split('JOIN|LEFT|RIGHT|ON', values[FROM]) if t.strip()]
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
                    separator = cls.get_separator(key)
                    obj.values[key] = [
                        Field.format(fld, obj)
                        for fld in re.split(separator, values[key])
                        if (fld != '*' and len(tables) == 1) or obj.match(fld)
                    ]
                result[obj.alias] = obj
        return list( result.values() )

    def optimize(self, rules: list[Rule]=None):
        if not rules:
            rules = Rule.__subclasses__()
        for rule in rules:
            rule.apply(self)


class SelectIN(Select):
    condition_class = Where

    def add(self, name: str, main: SQLObject):
        self.break_lines = False
        self.condition_class.list(self).add(name, main)

SubSelect = SelectIN

class NotSelectIN(SelectIN):
    condition_class = Not


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
            tokens = re.split(' or | OR ', re.sub('\n|\t|[()]', ' ', condition))
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
    REVERSE = {
        ">=": "<",
        "<=": ">",
        "<>": "=",
        "=": "<>"
    }
    @classmethod
    def apply(cls, target: Select):
        REGEX = re.compile('({})'.format(
            '|'.join(cls.REVERSE)
        ))
        for i, condition in enumerate(target.values.get(WHERE, [])):
            expr = re.sub('\n|\t', ' ', condition)
            tokens = [t for t in re.split(r'(NOT\b|not\b)',expr) if t.strip()]
            if len(tokens) < 2 or not REGEX.findall(tokens[-1]):
                continue
            tokens = REGEX.split(tokens[-1])
            tokens[1] = cls.REVERSE[tokens[1]]
            target.values[WHERE][i] = ' '.join(tokens)

class RuleDateFuncReplace(Rule):
    """
    SQL algorithm by Ralff Matias
    """
    REGEX = re.compile(r'(\bYEAR[(]|\byear[(]|=|[)])')

    @classmethod
    def apply(cls, target: Select):
        for i, condition in enumerate(target.values.get(WHERE, [])):
            tokens = [
                t.strip() for t in cls.REGEX.split(condition) if t.strip()
            ]
            if len(tokens) < 3:
                continue
            func, field, *rest, year = tokens
            temp = Select(f'{target.table_name} {target.alias}')
            Between(f'{year}-01-01', f'{year}-12-31').add(field, temp)
            target.values[WHERE][i] = ' AND '.join(temp.values[WHERE])
