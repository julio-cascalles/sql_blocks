from tests.tests import (
    best_movies, single_text_to_objects,
    detached_objects, many_texts_to_objects,
    query_reference
)

b = best_movies()
query = {}
expected_result = query_reference()

for func in [single_text_to_objects, detached_objects, many_texts_to_objects]:
    a, c, m = func()
    m.delete('director')
    m = m(id=b)
    query[func.__name__] = a + (m + c)


def test_single_text():
    assert query['single_text_to_objects'] == expected_result

def test_detached_obj():
    assert query['detached_objects'] == expected_result

def test_many_texts():
    assert query['many_texts_to_objects'] == expected_result
