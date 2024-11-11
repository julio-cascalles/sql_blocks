import re
from sql_blocks.sql_blocks import *

VOICE_TYPE_FIELD = 'voice_type'
VOICE_TYPE_VALUE = 'deep'


def error_inverted_condition() -> set:
    args = {VOICE_TYPE_FIELD: Not.eq(VOICE_TYPE_VALUE)}
    query = Select('Actor a', **args)
    return query.diff(WHERE, [f"{VOICE_TYPE_FIELD} <> '{VOICE_TYPE_VALUE}'"], True)

def named_fields_in_nested_query() -> list:
    query = Select(
        'Cast c',
        actor_id=Select(
            'Actor a',
            name=NamedField('actors_name'),
            id=PrimaryKey
        )
    )
    return query.values[SELECT]

def first_name_from_expr_field() -> str:
    query = Select('Actor a')
    field = ExpressionField(" LEFT({t}.{f}, POSITION(' ', {af}) ) AS first_{f}")
    return field.format('name', query)

def orderby_field_index() -> int:
    query = Select(
        "imdb_top_1000.csv movie",
        star1=[NamedField('actor'), GroupBy],
        imdb_Rating=Having.avg(Where.gt(8)),
        _=Count, _2=OrderBy
    )
    return int(
        re.findall(r'\d+', query.values[ORDER_BY][-1])[0]
    )

def many_fields_and_groups() -> dict:
    OrderBy.sort = SortType.ASC
    query = Select('post p')
    query.add_fields(
        'user_id, created_at',
        # ['user_id', 'created_at'],
        order_by=True, group_by=True
    )
    return query.values

def compare_individual_fields() -> bool:
    query = Select(
        'post p',
        user_id=[Field, GroupBy, OrderBy],
        created_at=[Field, GroupBy, OrderBy]
    )
    return query.values == many_fields_and_groups()

def added_object_changes() -> set:
    SQLObject.ALIAS_FUNC = lambda t: t[0]
    _class = Select(
        'class c',
        student_id=ForeignKey('student'),
        teacher_id=ForeignKey('teacher')
    )
    student = Select(student=Table('student_name, age'), id=PrimaryKey)
    teacher = Select(teacher=Table('teacher_name, course'), id=PrimaryKey)
    s1 = set( (student + (_class + teacher)).values[FROM] )
    s2 = set( _class.values[FROM] )
    return s1.intersection(s2)
