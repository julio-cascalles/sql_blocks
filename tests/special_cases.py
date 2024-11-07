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
