from tests.basic import (
    best_movies, single_text_to_objects,
    detached_objects, many_texts_to_objects,
    query_reference, two_queries_same_table,
    select_product, extract_subqueries
)
from tests.rules import (
    optimized_select_in,
    optimized_auto_field,
    optimized_logical_op,
    optimized_limit,
    optimized_date_func,
    all_optimizations
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


if __name__ == "__main__":
    print(query_reference())
