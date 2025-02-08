import re
from difflib import SequenceMatcher
from sql_blocks.sql_blocks import *


def diff_over_sum() -> set:
    Aggregate.break_lines = False
    query=Select(
        'Enrollment e',
        payment=Sum().over(
            student_id=Partition, due_date=OrderBy
        ).As('sum_per_student')
    )
    return query.diff(SELECT, ['OVER('], True)

def function_fields() -> list:
    query=Select(
        'Customers c',
        phone=[
            Not.is_null(),
            SubString(1, 4).As('area_code', GroupBy)
        ],
        customer_id=[
            Count().As('customer_count', OrderBy),
            Having.count(gt(5))
        ]
    )
    return query.values[SELECT]


def DateDiff_function_variants() -> dict:
    result = {}
    for dialect in Dialect:
        Function.dialect = dialect
        func = DateDiff(
            Current_Date(),
            'due_date'
        )
        result[dialect.name] = str(func)
    return result

def create_nested_functions() -> Select:
    query = Select(
        'Log', 
        event_date=[
            SubString( Cast('char'), 1, 10 ).As('day'),
            #           ^
            #           |
            #           +------- Nested function !!!
            Min().As('departure'),
            Max().As('arrival')
        ],
        day=GroupBy,
        resident=[Field, GroupBy]
    )
    cte = CTE('Traffic', [query])(
        event_date=SameDay('2024-10-03'),
        _=DateDiff('arrival', 'departure').As('time'),
        time=Lag().over(resident=Partition, day=OrderBy).As('Last')
    )
    return cte

def compare_nested_func_text(obj: Select) -> bool:
    txt1 = re.sub(r'\s+', ' ', str(obj) ).lower()
    txt2 = re.sub(r'\s+', ' ', """
        WITH Traffic AS (
            SELECT SubString(Cast(log.event_date As char), 1, 10) as day
            , Min(log.event_date) as departure, Max(log.event_date) as arrival
            , log.resident FROM Log log GROUP BY day, resident
        )SELECT
                arrival - departure as time,
                Lag(time) OVER(
                        PARTITION BY resident
                        ORDER BY day
                ) as Last
        FROM
                Traffic tra
        WHERE
                tra.event_date >= '2024-10-03 00:00:00' AND
                tra.event_date <= '2024-10-03 23:59:59'
    """).lower()
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66

def create_auto_convert_function() -> Select:
    return Select(
        'Person p',
        birth=Round( DateDiff(Current_Date()) ).As('age')
    )

def compare_auto_convert_text(obj: Select) -> bool:
    txt1 = obj.values[SELECT][-1].lower()
    txt2 = "Round(Cast(Current_Date() - p.birth As FLOAT)) as age".lower()
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66
