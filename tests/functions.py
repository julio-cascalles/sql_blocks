from sql_blocks.sql_blocks import *


def diff_over_sum() -> set:
    Aggregate.break_lines = False
    query=Select(
        'Enrollment e',
        payment=Sum().over(
            partition='student_id', order='due_date'
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