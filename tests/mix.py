import re
from sql_blocks.sql_blocks import (
    mix, Select, eq, gt, OrderBy, 
    Field, Table, PrimaryKey, 
    CypherParser, ForeignKey
)
from tests.util import create_public_schema


PERSON1_SCRIPT =  """
        SELECT name, age FROM person
        Where status = 'active'
    """

PERSON2_SCRIPT = """
        select department, name from PERSON where age > 18 ORDER BY name
    """

PIVOT_SCRIPT = """
SELECT     s.product,
           s.region, /* north_east    south_west */
           s.month_ref /* 
                    10=oct  11=nov  12=dec
            */
FROM    Sales s   GROUP BY    s.product
"""
SCRIPT_TO_JOIN = """
        SELECT
            o.ref_date, o.quantity
        FROM
            Order(person_id) -> person(id)
        order by
            o.ref_date DESC
"""


def p1_mix_p2() -> bool:
    q1 = mix(PERSON1_SCRIPT, PERSON2_SCRIPT)
    q2 = expected_mix(True, True)
    return q1 == q2

def expected_mix(p1_fields: bool = False, p2_fields: bool = False) -> Select:
    result = Select( person=Table('name, age') )
    if p1_fields:
        result(status=eq('active'))
    if p2_fields:
        result(
            department=Field,
            name=OrderBy,
            age=gt(18)
        )
    return  result

def pivot_summary() -> dict:
    result = {}
    query = mix(PIVOT_SCRIPT)
    arr = re.findall(r'Sum[(]CASE\s+WHEN\s+(.*)\bTHEN 1', str(query))
    for item in arr:
        found = re.findall(r"(\w+[.])*(\w+)\s*[=]\s*(.*)", item)
        if not found:
            continue
        _, field, value = found[0]
        result.setdefault(field, []).append(
            re.sub( r'[\'"]', '', value.strip() )
        )
    return result

def remove_mix() -> bool:
    q1 = mix(PERSON1_SCRIPT, 'where status is null', remove=True)
    q2 = expected_mix(False, False)
    return q1 == q2

def normal_join() -> Select:
    return Select(
        'Order', quantity=Field,
        person_id=expected_mix(True, False)(
            id=PrimaryKey
        ),
        ref_date=[Field, OrderBy.DESC]
    )

def compare_mix_join() -> bool:
    Select.ALIAS_FUNC = lambda t: t[0].lower()
    q1 = mix(PERSON1_SCRIPT, SCRIPT_TO_JOIN)
    q2 = normal_join()
    Select.ALIAS_FUNC = None
    return q1 == q2

def auto_complete_cypher() -> int:
    # ----------------------------------
    def show_difference(q1: Select, q2: Select):
        print('@'*100)
        print(q1)
        print('-'*100)
        print(q2)
        print('@'*100)
    # ----------------------------------
    create_public_schema()
    c1, s1, p1 = CypherParser(
        'c(na,re) <- s(q ^ref) -> p(na,pri)',
    Select).queries
    c2 = Select(
        'Customer c', id=PrimaryKey
    ).add_fields('name, region')
    if c1 != c2:
        show_difference(c1, c2)
        return 0
    p2 = Select(
        'Product p', serial_number=PrimaryKey
    ).add_fields('name, price')
    if p1 != p2:
        show_difference(p1, p2)
        return 1
    s2 = Select(
        'Sales s', quantity=Field, 
        ref_date=[OrderBy, Field],         
        pro_id=ForeignKey('Product'),
        cus_id=ForeignKey('Customer')
    )
    if s1 != s2:
        show_difference(s1, s2)
        return 2
    return 3
