from enum import Enum
import re


PATTERN_PREFIX = '([^0-9 ]+[.])'
PATTERN_SUFFIX = '( [A-Za-z_]+)'
DISTINCT_PREFX = '(DISTINCT|distinct)'

KEYWORD = {
    'SELECT': (',{}', 'SELECT *', DISTINCT_PREFX),
    'FROM': ('{}', '', PATTERN_SUFFIX),
    'WHERE': ('{}AND ', '', r'["\']'),
    'GROUP BY': (',{}', '', PATTERN_SUFFIX),
    'ORDER BY': (',{}', '', PATTERN_SUFFIX),
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

    def diff(self, key: str, search_list: list, exact: bool=False) -> set:
        def disassemble(source: list) -> list:
            result = []
            for fld in source:
                result += re.split(r'([=()]|<>|\s+ON\s+|\s+on\s+)', fld)
            return result
        def cleanup(fld: str) -> str:
            if exact:
                fld = fld.lower()
            return fld.strip()
        def is_named_field(fld: str) -> bool:
            return key == SELECT and re.search(' as | AS ', fld)
        def field_set(source: list) -> set:
            return set(
                (
                    fld if is_named_field(fld) else
                    re.sub(pattern, '', cleanup(fld))
                )
                for string in disassemble(source)
                for fld in re.split(separator, string)
            )       
        pattern = KEYWORD[key][2] 
        if exact:
            if key == WHERE:
                pattern = ' '
            pattern += f'|{PATTERN_PREFIX}'
        separator = self.get_separator(key)
        s1 = field_set(search_list)
        s2 = field_set(self.values.get(key, []))
        if exact:
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
        self.class_type = class_type

    def add(self, name: str, main: SQLObject):
        main.values.setdefault(SELECT, []).append(
            '{} as {}'.format(
                self.class_type.format(name, main),
                self.alias  # --- field alias
            )
        )


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

    def __init__(self, fields: list=[], class_types = [Field]):
        if isinstance(fields, str):
            fields = [
                f.strip() for f in fields.split(self.separator)
            ]
        self.fields = fields
        self.class_types = class_types

    def add(self, name: str, main: SQLObject):
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
    def contains(cls, value: str):
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
    def inside(cls, values):
        if isinstance(values, list):
            values = ','.join(quoted(v) for v in values)
        return cls(f'IN ({values})')

    def add(self, name: str, main: SQLObject):
        main.values.setdefault(WHERE, []).append('{}{} {}'.format(
            self.prefix, Field.format(name, main), self.expr
        ))


eq, contains, gt, gte, lt, lte, is_null, inside = (
    getattr(Where, method) for method in 
    ('eq', 'contains', 'gt', 'gte', 'lt', 'lte', 'is_null', 'inside')
) 


class Not(Where):
    prefix = 'NOT '

    @classmethod
    def eq(cls, value):
        return Where(expr=f'<> {quoted(value)}')


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
        found = re.findall(r'^_\d', name)
        if found:
            name = found[0].replace('_', '')
        elif main.alias:
            name = f'{main.alias}.{name}'
        main.values.setdefault(ORDER_BY, []).append(name+cls.sort.value)


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
        return self.REGEX['separator'].split(
            self.remove_spaces(txt)
        )


class SQLParser(Parser):
    REGEX = {}

    def prepare(self):
        keywords = '|'.join(k + r'\b' for k in KEYWORD)
        flags = re.IGNORECASE + re.MULTILINE
        self.REGEX['keywords'] = re.compile(f'({keywords}|[*])', flags)
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
                    separator = self.class_type.get_separator(key)
                    obj.values[key] = [
                        Field.format(fld, obj)
                        for fld in re.split(separator, values[key])
                        if (fld != '*' and len(tables) == 1) or obj.match(fld)
                    ]
                result[obj.alias] = obj
        self.queries = list( result.values() )


class Cypher(Parser):
    REGEX = {}

    def prepare(self):
        self.REGEX['separator'] = re.compile(r'([(,?)^{}[\]]|->|<-)')
        self.REGEX['condition'] = re.compile(r'(^\w+)|([<>=])')
        self.join_type = JoinType.INNER
        self.TOKEN_METHODS = {
            '(': self.add_field,  '?': self.add_where,
            ',': self.add_field,  '^': self.add_order,
            ')': self.new_query,  '->': self.left_ftable,
            '<-': self.right_ftable,
        }
        self.method = self.new_query

    def new_query(self, token: str):
        if token.isidentifier():
            self.queries.append( self.class_type(token) )

    def add_where(self, token: str):
        field, *condition = [
            t for t in self.REGEX['condition'].split(token) if t
        ]
        Where(' '.join(condition)).add(field, self.queries[-1])
    
    def add_order(self, token: str):
        FieldList(token, [Field, OrderBy]).add('', self.queries[-1])

    def add_field(self, token: str):
        FieldList(token, [Field]).add('', self.queries[-1])

    def left_ftable(self, token: str):
        self.new_query(token)
        self.join_type = JoinType.LEFT

    def right_ftable(self, token: str):
        self.new_query(token)
        self.join_type = JoinType.RIGHT

    def add_foreign_key(self, token: str, pk_field: str=''):
        curr, last = [self.queries[i] for i in (-1, -2)]
        if not pk_field:
            if not last.values.get(SELECT):
                return
            pk_field = last.values[SELECT][-1].split('.')[-1]
            last.delete(pk_field, [SELECT])
        if self.join_type == JoinType.RIGHT:
            curr, last = last, curr
        if '{}' in token:
            token = token.format(
                curr.table_name.lower()
            )
        k = ForeignKey.get_key(last, curr)
        ForeignKey.references[k] = (token, pk_field)
        self.join_type = JoinType.INNER

    def eval(self, txt: str):
        for token in self.get_tokens(txt):
            if not token:
                continue
            if self.method:
                self.method(token)
            if token in '([' and self.join_type != JoinType.INNER:
                self.method = self.add_foreign_key
            else:
                self.method = self.TOKEN_METHODS.get(token)

class Neo4JParser(Cypher):
    def prepare(self):
        super().prepare()
        self.TOKEN_METHODS = {
            '(': self.new_query,  '{': self.add_where,
            '->': self.left_ftable, '<-': self.right_ftable,
            '[': self.new_query
        }
        self.method = None

    def new_query(self, token: str):
        super().new_query(token.split(':')[-1])

    def add_where(self, token: str):
        super().add_where(token.replace(':', '='))

    def add_foreign_key(self, token: str, pk_field: str='') -> tuple:
        self.new_query(token)
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
        return Where

    def increment_brackets(self, value: str):
        self.brackets[value] += 1

    def eval(self, txt: str):
        self.method = self.new_query
        self.last_field = ''
        self.where_list = None
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
            self.method = self.TOKEN_METHODS.get(token)
# ----------------------------


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
        old_tables = main.values.get(FROM, [])
        new_tables = set([
            '{jt}JOIN {tb} {a2} ON ({a1}.{f1} = {a2}.{f2})'.format(
                jt=self.join_type.value,
                tb=self.table_name,
                a1=main.alias, f1=name,
                a2=self.alias, f2=self.key_field
            )
        ] + old_tables[1:])
        main.values[FROM] = old_tables[:1] + list(new_tables)
        for key in USUAL_KEYS:
            main.update_values(key, self.values.get(key, []))

    def __add__(self, other: SQLObject):
        from copy import deepcopy
        query = deepcopy(self)
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
            raise ValueError(f'No relationship found between {query.table_name} and {other.table_name}.')
        elif primary_key:
            PrimaryKey.add(primary_key, other)
        other.add(foreign_field, query)
        return query

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
    def parse(cls, txt: str, parser: Parser = SQLParser) -> list[SQLObject]:
        return parser(txt, cls).queries

    def optimize(self, rules: list[Rule]=None):
        if not rules:
            rules = Rule.__subclasses__()
        for rule in rules:
            rule.apply(self)

    def add_fields(self, fields: list, order_by: bool=False, group_by:bool=False):
        class_types = [Field]
        if order_by:
            class_types += [OrderBy]
        if group_by:
            class_types += [GroupBy]
        FieldList(fields, class_types).add('', self)



class SelectIN(Select):
    condition_class = Where

    def add(self, name: str, main: SQLObject):
        self.break_lines = False
        self.condition_class.inside(self).add(name, main)

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
