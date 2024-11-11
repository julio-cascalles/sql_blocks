from tests.basic import (
    best_movies, single_text_to_objects,
    detached_objects, many_texts_to_objects,
    query_reference, two_queries_same_table,
    select_product, extract_subqueries,
    select_expression_field, is_expected_expression, 
    EXPR_ARR1, EXPR_ARR2
)
from tests.rules import (
    optimized_select_in,
    optimized_auto_field,
    optimized_logical_op,
    optimized_limit,
    optimized_date_func,
    all_optimizations
)
from tests.special_cases import (
    error_inverted_condition, named_fields_in_nested_query,
    first_name_from_expr_field, orderby_field_index,
    many_fields_and_groups, compare_individual_fields,
    added_object_changes
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
