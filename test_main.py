from tests.basic import (
    best_movies, single_text_to_objects,
    detached_objects, many_texts_to_objects,
    query_reference, two_queries_same_table,
    select_product, extract_subqueries,
    select_expression_field, is_expected_expression, 
    EXPR_ARR1, EXPR_ARR2, like_conditions
)
from tests.rules import (
    optimized_select_in,
    optimized_auto_field,
    optimized_logical_op,
    optimized_limit,
    optimized_date_func,
    all_optimizations, 
    replace_join_by_subselect
)
from tests.special_cases import (
    error_inverted_condition, named_fields_in_nested_query,
    first_name_from_expr_field, orderby_field_index,
    many_fields_and_groups, compare_individual_fields,
    added_object_changes, query_for_cypher, cypher_query,
    mongo_query, query_for_mongo, mongo_group, group_for_mongo,
    neo4j_queries, query_for_neo4J, neo4j_joined_query,
    script_from_neo4j_query, script_mongo_from,
    neo4j_with_WHERE, query_for_WHERE_neo4j, 
    group_cypher, cypher_group, detected_parser_classes,
    compare_join_condition, tables_without_JOIN,
    customers_without_orders, range_conditions,
    SQLINJECTION_FALSE_POSITIVES, SQLINJECTION_REAL_ATTEMPT,
    is_sql_injection
)
from tests.functions import (
    diff_over_sum, function_fields,
    DateDiff_function_variants,
    create_nested_functions, compare_nested_func_text,
    create_auto_convert_function, compare_auto_convert_text
)
from tests.cte import(
    basic_recursive_cte, compare_basic_recursive,
    create_flight_routes, compare_created_routes,
    compare_factory_result, compare_query_list
)


_best_movies = best_movies()
query = {}
expected_result = query_reference()
subqueries = extract_subqueries()


for func in [single_text_to_objects, detached_objects, many_texts_to_objects]:
    a, c, m = func()
    m.delete('director')
    m = m(id=_best_movies)
    query[func.__name__] = a + (m + c)


def test_single_text():
    assert query['single_text_to_objects'] == expected_result

def test_detached_obj():
    assert query['detached_objects'] == expected_result

def test_many_texts():
    assert query['many_texts_to_objects'] == expected_result

def test_same_table():
    p1 = two_queries_same_table()
    p2 = select_product()
    assert p1 == p2

def test_subquery_Genres():
    g = subqueries['Genres']
    assert g.__class__.__name__ == 'NotSelectIN'
    
def test_subquery_Review():
    assert subqueries['Review'] == _best_movies

def test_rule_select_in():
    assert optimized_select_in()

def test_rule_auto_field():
    assert optimized_auto_field()

def test_rule_logical_op():
    assert optimized_logical_op()

def test_rule_put_limit():
    assert optimized_limit()

def test_rule_date_func_replace():
    assert optimized_date_func()

def test_all_optimizations():
    assert all_optimizations()

def test_expression_field():
    assert is_expected_expression(
        select_expression_field(False), EXPR_ARR1
    )

def test_named_expr_fld():
    assert is_expected_expression(
        select_expression_field(True), EXPR_ARR2
    )

def test_inverted_condition():
    assert not error_inverted_condition()

def test_named_fields_nested_query():
    assert named_fields_in_nested_query() == ['a.name as actors_name']

def test_complex_expression_field():
    EXPECTED_FLD = " LEFT(Actor.name, POSITION(' ', a.name) ) AS first_name"
    assert first_name_from_expr_field() == EXPECTED_FLD

def test_orderby_field_index():
    assert orderby_field_index() == 2

def test_many_fields_and_groups():
    EXPECTED_FIELDS = ["p.user_id", "p.created_at"]
    res = many_fields_and_groups()    
    for key in ('SELECT', 'GROUP BY', 'ORDER BY'):
        assert res[key] == EXPECTED_FIELDS

def test__compare_individual_fields():
    assert compare_individual_fields()

def test_added_object_changes():
    assert added_object_changes() == {'class c'}

def test_cypher():
    q1, q2 = query_for_cypher(), cypher_query()
    assert q1 == q2
    """
    Check that there is no error when
     running the same test twice.
    """
    q1, q2 = query_for_cypher(), cypher_query()
    assert q1 == q2

def test_mongo_query():
    q1, q2 = query_for_mongo(), mongo_query()
    assert q1 == q2

def test_group_mongo():
    q1, q2 = mongo_group(), group_for_mongo()
    assert q1 == q2

def test_neo4J():
    q1, q2 = neo4j_joined_query(), query_for_neo4J()
    assert q1 == q2

def test_neo4J_round_trip():
    s1, c1, t1 = neo4j_queries()
    s2, c2, t2 = neo4j_queries(
        script_from_neo4j_query()
    )
    assert all([
        s1 == s2,
        c1 == c2,
        t1 == t2
    ])

def test_mongo_round_trip():
    q1 = mongo_query()
    q2 = mongo_query(
        script_mongo_from(q1)
    )
    assert q1 == q2

def test_mongo_group_rtrip():
    q1 = mongo_group()
    q2 = mongo_group(
        script_mongo_from(q1)
    )
    assert q1 == q2

def test_neo4J_with_WHERE():
    q1 = neo4j_with_WHERE()
    q2 = query_for_WHERE_neo4j()
    assert q1 == q2

def test_group_cypher():
    q1 = group_cypher()
    q2 = cypher_group()
    print('='*50)
    print(q1)
    print('-'*50)
    print(q2)
    assert q1 == q2

def test_parser_classes():
    assert detected_parser_classes()

def test_over():
    expected = {
        'sum',
        'partition by student_id order by due_date',
        'as sum_per_student'
    }
    res = diff_over_sum()
    print('@'*50)
    print(res)
    print('-'*50)
    print(res.intersection(expected))
    print('@'*50)
    assert res.intersection(expected) == expected

def test_function_fields():
    expected = [
        'SubString(c.phone, 1, 4) as area_code',
        'Count(c.customer_id) as customer_count'
    ]
    assert function_fields() == expected

def test_dialects():
    expected = {
        "ANSI": "Current_Date() - due_date",
        "SQL_SERVER": "DateDiff(getDate(), due_date)",
        "ORACLE": "SYSDATE - due_date",
        "POSTGRESQL": "Current_date - due_date",
        "MYSQL": "Current_Date() - due_date"
    }
    assert DateDiff_function_variants() == expected

def test_rule_replace_join_by_subselect():
    expected = ["i.customer IN (SELECT c.id FROM Customer c WHERE c.name LIKE 'Albert E%')"]
    assert replace_join_by_subselect() == expected

def test_cte():
    r = basic_recursive_cte()
    assert compare_basic_recursive(r)

def test_cte_with_counter():
    r = basic_recursive_cte(True)
    assert compare_basic_recursive(r, True)

def test_multiple_tables_without_JOIN():
    twj = tables_without_JOIN()
    assert compare_join_condition(twj)

def test_like_conditions():
    assert like_conditions() == [
        "'Julio%'", "'%Cesar%'", "'%Cascalles'",
    ]

def test_create_recursive():
    r = create_flight_routes()
    assert compare_created_routes(r)

def test_create_joined_recursive():
    r = create_flight_routes(True)
    assert compare_created_routes(r, True)

def test_nested_functions():
    obj = create_nested_functions()
    assert compare_nested_func_text(obj)

def test_auto_convert_func():
    obj = create_auto_convert_function()
    assert compare_auto_convert_text(obj)

def test_query_diff():
    q1, q2 = [
        customers_without_orders(val)
        for val in (True, False)
    ]
    assert q1 == q2

def test_range_conditions():
    range_list = [
        "BETWEEN 0 AND 10 THEN 'child'",
        "BETWEEN 11 AND 17 THEN 'teenager'",
        "BETWEEN 18 AND 21 THEN 'young'",
        "BETWEEN 22 AND 50 THEN 'adult'",
        "BETWEEN 51 AND 70 THEN 'elderly'",
    ]
    assert range_conditions() == range_list

def test_false_sql_injection():
    for word in SQLINJECTION_FALSE_POSITIVES:
        assert not is_sql_injection(word)

def test_real_sql_injection():
    assert is_sql_injection(SQLINJECTION_REAL_ATTEMPT)

def test_factory1():
    assert compare_factory_result(1)

def test_factory2():
    assert compare_factory_result(2)

def test_cte_query_list():
    assert compare_query_list()