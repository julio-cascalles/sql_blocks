import re
from difflib import SequenceMatcher
from sql_blocks.sql_blocks import *


def compare_basic_recursive(obj: Recursive, use_counter: bool=False) -> bool:
    txt1 = re.sub(r'\s+', ' ', str(obj) ).lower()
    START_COUNTER = ', 5 AS generation'
    COUNTER_INCREMENT = ', (generation-1) AS generation'
    txt2 = re.sub(r'\s+', ' ', f"""
        WITH RECURSIVE ancestors AS (
            SELECT f1.id, f1.name, f1.father, f1.mother, f1.birth
            {START_COUNTER if use_counter else ''}
            FROM Folks f1 WHERE f1.id = 32630
        UNION ALL
            SELECT f2.id, f2.name, f2.father, f2.mother, f2.birth
            {COUNTER_INCREMENT if use_counter else ''}
            FROM Folks f2 , ancestors a WHERE (f2.id = a.father OR f2.id = a.mother)
        )SELECT * FROM ancestors
    """).lower()
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66

def basic_recursive_cte(use_counter: bool=False):
    """
    Manually create a Recursive object:
    """
    def get_query(i: int):
        query = Select(f"Folks f{i}")
        query.add_fields('id,name,father,mother,birth')
        return query
    q1, q2 = [get_query(i) for i in (1, 2)]
    R = Recursive('ancestors a', [
        q1(id=eq(32630)), q2(
            id=Where.formula('(% = a.father OR % = a.mother)')
        )
    ])
    if use_counter:
        R.counter('generation', 5, '- 1')
    return R

def create_flight_routes(join_airport: bool = False) -> Recursive:
    FLIGHT_FIELDS = 'departure, arrival'
    R = Recursive.create(
        'Route R', f'Flyght({FLIGHT_FIELDS})',
        '[2] = R.[1]', 'JFK', '.csv'
    )
    if join_airport:
        R.join('Airport(*id,name)', FLIGHT_FIELDS, format='.csv')
    return R

def compare_created_routes(obj: Recursive, join_airport: bool = False) -> bool:
    txt1 = re.sub(r'\s+', ' ', str(obj) ).lower()
    AIRPORT_TABLES = """
        SELECT
            a1.name, a2.name
        FROM
            Route R
            JOIN Airport.csv a1 ON (R.departure = a1.id)
            JOIN Airport.csv a2 ON (R.arrival = a2.id)
    """
    SIMPLE_ROUTE_SELECT = 'SELECT * FROM Route R'
    txt2 = re.sub(r'\s+', ' ', f"""
        WITH RECURSIVE Route AS (
            SELECT f1.departure, f1.arrival FROM Flyght.csv f1
            WHERE f1.departure = 'JFK'
        UNION ALL
            SELECT f2.departure, f2.arrival FROM Flyght.csv f2 , Route R
            WHERE f2.arrival = R.departure
        ){AIRPORT_TABLES if join_airport else SIMPLE_ROUTE_SELECT}
    """).lower()
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66


def get_query_list(count: int) -> tuple:
    def union_expr(source: list) -> str:
        return '\n\tUNION ALL\n'.join(source[:count])
    # ----------------------------------------------------
    USERS = [
        "SELECT u.name, u.role FROM Users u WHERE u.role = 'employee'",
        "SELECT u.name, u.role FROM Users u WHERE u.role = 'customer'"
    ]
    SALES = [
        """SELECT s.user_id, Sum(s.value) as total, 'dec-24' as period
           FROM Sales s WHERE ref_date = '2024-12-27' GROUP BY s.user_id
        """,
        """SELECT s.user_id, Sum(s.value) as total, 'jan-25' as period
           FROM Sales s WHERE ref_date = '2025-01-31' GROUP BY s.user_id
        """
    ]
    # ----------------------------------------------------
    return (
        union_expr(USERS), union_expr(SALES)
    )

def factory_result(query_count: int) -> CTEFactory:
    users, sales = get_query_list(query_count)
    return CTEFactory(f"""
        SELECT u001.name, agg_sales.total
        FROM (
            {users}
        ) AS u001
        JOIN (
            {sales}
        )
        As agg_sales
        ON u001.id = agg_sales.user_id
        ORDER BY u001.name
    """)        

def expected_from_factory(query_count: int) -> str:
    users, sales = get_query_list(query_count)
    return f"""
    WITH u001 AS (
        {users}
    ),
    WITH agg_sales AS (
        {sales}
    )
    SELECT
            u001.name,
            agg_sales.total
    FROM
            u001 u001
            JOIN agg_sales agg_sales ON
            (u001.id = agg_sales.user_id)
    ORDER BY
            u001.name
    """

def compare_factory_result(count: int) -> bool:
    txt1, txt2 = [
        re.sub(r'\s+', ' ', str(res)).strip()
        for res in [factory_result(count), expected_from_factory(count)]
    ]
    txt1 = re.sub('\bLEFT\b', '', txt1)
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66

def compare_query_list() -> bool:
    ctes = factory_result(2).cte_list
    if len(ctes) != 2:
        return False
    return all(len(c.query_list) == 2 for c in ctes)
