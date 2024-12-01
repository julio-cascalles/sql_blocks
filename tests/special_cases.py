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

def query_for_cypher() -> Select:
    return Select(
        'Class c', student_id=Select(
            'Student s', name=Field, age=eq(16), enrollment=PrimaryKey
        ), teacher_id=Select(
            'Teacher t', social_security=PrimaryKey,
            name=Field, subject=[Field, OrderBy]
        )
    )

def cypher_query() -> Select:
    SQLObject.ALIAS_FUNC = lambda t: t[0].lower()
    CYPHER_SCRIPT = """
        Student(
            name ? age = 16, enrollment
        ) <- Class(
            student_id,
            teacher_id
        ) ->
        Teacher(
            social_security, name ^subject
        )
    """
    student, _class, teacher = Select.parse(CYPHER_SCRIPT, Cypher)
    return student + _class + teacher

MONGODB_SCRIPT_FIND = '''
    db.people.find(
        {
            { $or : [
                    {status: "B"}, {age:50} 
                    ] 
            },
            age:{$gte: 18}, {status: "A"}
        }, {name: 1, user_id: 1}
    ).sort({user_id: -1}) 
'''
MONGO_SCRIPT_AGGREG = '''
    db.people.aggregate([
        {"$group" : {_id:"$gender", count:{$sum:1}}}
    ])    
'''
NEO4J_SCRIPT = '''
    MATCH (s: Student)
    <- [:Class] ->
    (t: Teacher{name:"Joey Tribbiani"})
    RETURN s, t
'''

def mongo_query() -> Select:
    return Select.parse(MONGODB_SCRIPT_FIND, MongoParser)[0]

def query_for_mongo() -> Select:
    return Select(
        'People',
        OR=Options(status=eq('B'), age=eq(50)),
        age=gte(18), status=eq('A'),
        name=Field,
        user_id=[Field, OrderBy],
    )

def mongo_group() -> Select:
    return Select.parse(MONGO_SCRIPT_AGGREG, MongoParser)[0]

def group_for_mongo() -> Select:
    return Select(people=Table('sum(1)'), gender=GroupBy)

def neo4j_queries(script: str = NEO4J_SCRIPT) -> list:
    print('@'*50)
    print('Neo4J script:\n', script)
    print('@'*50)
    return Select.parse(script, Neo4JParser)

def neo4j_joined_query() -> Select:
    student, _class, teacher = neo4j_queries()
    return student + _class + teacher

def query_for_neo4J() -> Select:
    return Select(
        'Class c', student_id=Select(
            'Student s', id=PrimaryKey,
        ), teacher_id=Select(
            'Teacher t', id=PrimaryKey,
            name=eq('Joey Tribbiani')
        )
    )

def script_from_neo4j_query() -> str:
    # return query_for_neo4J().translate_to(Neo4JLanguage)
    return neo4j_joined_query().translate_to(Neo4JLanguage)