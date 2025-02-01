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
    R = Recursive.create(
        'Route R', 'Flyght(departure, arrival)',
        '[2] = R.[1]', 'JFK', '.csv'
    )
    if join_airport:
        R.join('Airport(*id,name)', 'departure, arrival', format='.csv')
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
