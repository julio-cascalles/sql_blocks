import re
from difflib import SequenceMatcher
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
        [OrderBy, GroupBy]
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
        'class',
        student_id=ForeignKey('student'),
        teacher_id=ForeignKey('teacher')
    )
    student = Select(student=Table('student_name, age'), id=PrimaryKey)
    teacher = Select(teacher=Table('teacher_name, course'), id=PrimaryKey)
    s1 = set( (student + (_class + teacher)).values[FROM] )
    s2 = set( _class.values[FROM] )
    SQLObject.ALIAS_FUNC = None
    return s1.intersection(s2)

def query_for_cypher() -> Select:
    right_query=Select(
        'Teacher t', social_security=PrimaryKey,
        name=Field, subject=[Field, OrderBy]
    )
    right_query.join_type = JoinType.RIGHT
    return Select(
        'Class c', student_id=Select(
            'Student s', name=Field, age=eq(16), enrollment=PrimaryKey
        ), teacher_id=right_query
    )

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
MONGODB_SCRIPT_FIND = '''
    db.people.find({
            {
                $or: [
                    status:{$eq:'B'},
                    age:{$lt:50}
                ]
            },
            age:{$gte:18},  status:{$eq:'A'}
    },{
            name: 1, user_id: 1
    }).sort({
            user_id: -1
    })
'''
MONGO_SCRIPT_AGGREG = '''
    db.people.aggregate([
        {"$group" : {_id:"$gender", count:{$sum:1}}}
    ])    
'''
NEO4J_FORMAT = '''
    MATCH 
        (s: Student)
            <- [:Class] ->
        (t: Teacher{})
    {}RETURN s, t
'''
NEO4J_SCRIPT = NEO4J_FORMAT.format(
    '{name:"Joey Tribbiani"}', ''
)

def cypher_query() -> Select:
    ForeignKey.references = {}
    student, _class, teacher = Select.parse(CYPHER_SCRIPT, CypherParser)
    return student + _class + teacher

def mongo_query(script: str = MONGODB_SCRIPT_FIND) -> Select:
    return Select.parse(script, MongoParser)[0]

def query_for_mongo() -> Select:
    return Select(
        'People',
        OR=Options(status=eq('B'), age=lt(50)),
        age=gte(18), status=eq('A'),
        name=Field,
        user_id=[Field, OrderBy],
    )

def mongo_group(script: str=MONGO_SCRIPT_AGGREG) -> Select:
    return Select.parse(script, MongoParser)[0]

def group_for_mongo() -> Select:
    return Select(people=Table('sum(1)'), gender=GroupBy)

def neo4j_queries(script: str = NEO4J_SCRIPT) -> list:
    return Select.parse(script, Neo4JParser)

def neo4j_joined_query() -> Select:
    student, _class, teacher = neo4j_queries()
    return student + _class + teacher

def left_query_neo4j() -> Select:
    return Select(
        'Student s', id=PrimaryKey,
    )

def query_for_neo4J(where_func=eq, left_query=None) -> Select:
    right_query = Select(
        'Teacher t', id=PrimaryKey,
        name=where_func('Joey Tribbiani')
    )
    if not left_query:
        left_query = left_query_neo4j()
    right_query.join_type = JoinType.RIGHT
    return Select(
        'Class c', student_id=left_query, teacher_id=right_query
    )

def script_from_neo4j_query() -> str:
    return neo4j_joined_query().translate_to(Neo4JLanguage)

def script_mongo_from(query: Select) -> str:
    return query.translate_to(MongoDBLanguage)

def neo4j_with_WHERE() -> list[Select]:
    WHERE_APPENDIX = '''WHERE
        s.age > 18 AND
        t.name <> "Joey Tribbiani"
    '''
    txt = NEO4J_FORMAT.format('', WHERE_APPENDIX)
    s, c, t = neo4j_queries(txt)
    return (s + c + t)

def query_for_WHERE_neo4j():
    query = left_query_neo4j()
    query(age=gt(18))
    return query_for_neo4J(Not.eq, query)

def group_cypher() -> Select:
    return Select(
        'People', age=Avg, gender=[GroupBy, Field],
        id=Count().As('qtde', OrderBy), region=eq('SOUTH')
    )

def cypher_group() -> Select:
    return detect('People@gender(avg$age?region="SOUTH"^count$:qtde)')

def detected_parser_classes() -> bool:
    CASES = [
        ('SELECT * FROM product',  SQLParser),
        (MONGODB_SCRIPT_FIND,  MongoParser),
        (NEO4J_SCRIPT,  Neo4JParser),
        (CYPHER_SCRIPT,  CypherParser),
    ]
    for script, class_type in CASES:
        pc =  parser_class(script)
        if pc != class_type:
            return False
    return True

def compare_join_condition(obj: Select) -> bool:
    txt1 = re.sub(r'\s+', ' ', str(obj) ).lower()
    txt2 = re.sub(r'\s+', ' ', """
        SELECT
                album.name as album_name,
                singer.name as artist_name,
                album.year_recorded
        FROM
                'sql_blocks/music/data/Album.csv' album
                ,'sql_blocks/music/data/Singer.csv' singer
        WHERE
                (album.artist_id = singer.id)
    """).lower()
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66

def tables_without_JOIN() -> Select:
    SQLObject.ALIAS_FUNC = lambda t: t.lower()
    singer = Select(
        " 'sql_blocks/music/data/Singer.csv' ", name=NamedField('artist_name'),
        id=PrimaryKey
    )
    album = Select (
        " 'sql_blocks/music/data/Album.csv' ", name=NamedField('album_name'),
        artist_id=Where.join(singer),
        year_recorded=Field
    )
    return album

def customers_without_orders(subtract: bool) -> Select:
    TABLE_CUSTOMERS = 'customers c'
    TABLE_ORDERS = 'orders o'
    STATUS_DELIVERED_OK = 93
    if subtract:
        orders = Select(TABLE_ORDERS,
            customer_id=ForeignKey(TABLE_CUSTOMERS.split()[0])
        )
        customers = Select(TABLE_CUSTOMERS, id=PrimaryKey, name=Field)
        return customers - orders(status=eq(STATUS_DELIVERED_OK))
    return Select(
        TABLE_CUSTOMERS, name=Field,
        id=NotSelectIN(
            TABLE_ORDERS, customer_id=Field, status=eq(STATUS_DELIVERED_OK)
        )
    )

def range_conditions() -> list:
    query = Select(
        'People p',
        age_group=Range('age',{
            'adult': 50,
            'teenager': 17,
            'child': 10,
            'elderly': 70,
            'young': 21,
        })
    )
    return re.findall(r"BETWEEN \d+ AND \d+ THEN '\w+'", str(query))


SQLINJECTION_FALSE_POSITIVES =[
    'world','major','north','order','short',
    'force','story','worth','floor','store'
]
SQLINJECTION_REAL_ATTEMPT = "AAA'OR 1=1"

def is_sql_injection(value: str) -> bool:
    try:
        quoted(value)
    except PermissionError as e:
        return True
    return False