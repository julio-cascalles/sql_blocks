from sql_blocks.sql_blocks import *

VOICE_TYPE_FIELD = 'voice_type'
VOICE_TYPE_VALUE = 'deep'


def error_inverted_condition() -> set:
    args = {VOICE_TYPE_FIELD: Not.eq(VOICE_TYPE_VALUE)}
    query = Select('Actor a', **args)
    return query.diff(WHERE, [f"{VOICE_TYPE_FIELD} <> '{VOICE_TYPE_VALUE}'"], True)

def named_field_in_nested_query() -> str:
    query = Select(
        'Cast c',
        actor_id=Select(
            'Actor a',
            name=NamedField('actors_name'),
            id=PrimaryKey
        )
    )
    return query.values[SELECT]
