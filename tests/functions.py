import re
from difflib import SequenceMatcher
from sql_blocks.sql_blocks import *


def diff_over_sum() -> set:
    OrderBy.sort = SortType.ASC
    Aggregate.break_lines = False
    query=Select(
        'Enrollment e',
        payment=Sum().over(
            student_id=Partition, due_date=OrderBy
        ).As('sum_per_student')
    )
    return query.diff(SELECT, [], True)

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

SCRIPT_FUNCTIONS = '''SELECT SubString(Cast(log.event_date As char), 1, 10) as day
            , Min(log.event_date) as departure, log.resident, Max(log.event_date) as arrival
            FROM Log log GROUP BY day, resident
'''


def compare_nested_func_text(obj: Select) -> bool:
    txt1 = re.sub(r'\s+', ' ', str(obj) ).lower()
    txt2 = re.sub(r'\s+', ' ', f"""
        WITH Traffic AS (
            {SCRIPT_FUNCTIONS}
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

def function_list() -> list:
    result = set()
    def all_functions(node: FuncNode):
        result.add(node.func_name)
    FuncNode.create(SCRIPT_FUNCTIONS, all_functions)
    return result

def window_func_with_formula() -> bool:
    FORMULA = '(curr_value - prev_value) / prev_value'
    FIELDS = 'customer, curr_value, ref_date'
    OVER_PARAMS = dict(
            ref_date=OrderBy,
            customer=Partition,
        )
    q1 = Select(
        Investiment=Table(FIELDS),
        curr_value=Lag().over(**OVER_PARAMS).As('prev_value'),
        _=ExpressionField(FORMULA+'  AS variation'),
    )
    q2 = Select(
        Investiment=Table(FIELDS),
        variation=Lag(FORMULA).over(**OVER_PARAMS),
    )
    return q1 == q2
