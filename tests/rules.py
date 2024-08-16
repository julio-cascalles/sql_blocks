from sql_blocks.sql_blocks import *

PRODUCT_TABLE = 'Product p'


def select_by_category(modifiers: list) -> Select:
    result = Select(PRODUCT_TABLE)
    for m in modifiers:
        m.add('category', result)
    return result

def optimized_select_in() -> bool:
    p1 = Select.parse(f"""
        SELECT * FROM {PRODUCT_TABLE}
		WHERE (p.category = 'Gizmo'
		OR p.category = 'Gadget'
		OR p.category = 'Doohickey')
    """)[0]
    p1.optimize([RuleSelectIN])
    p2 = select_by_category([
        Where.list([
            'Gizmo', 'Gadget', 'Doohickey'
        ])
    ])
    print('@'*100)
    print(p1.values)
    print('#'*100)
    print(p2.values)
    print('@'*100)
    return p1 == p2

def optimized_auto_field() -> bool:
    p1 = select_by_category([GroupBy])
    p1.optimize([RuleAutoField])
    p2 = select_by_category([GroupBy, Field])
    return p1 == p2

def optimized_logical_op() -> bool:
    REF_PRICE = 387.64
    p1 = Select(PRODUCT_TABLE, price=Not.lte(REF_PRICE))
    p2 = Select(PRODUCT_TABLE, price=Where.gt(REF_PRICE))
    p1.optimize([RuleLogicalOp])
    print('@'*100)
    print(p1.values)
    print('='*100)
    print(p2.values)
    print('@'*100)
    return p1 == p2

def optimized_limit() -> bool:
    p1 = Select(PRODUCT_TABLE)
    p2 = Select(PRODUCT_TABLE).limit()
    p1.optimize([RulePutLimit])
    return p1 == p2

def optimized_date_func() -> bool:
    p1 = Select.parse(
        f'SELECT * FROM {PRODUCT_TABLE} WHERE YEAR(last_sale) = 2024'
    )[0]
    p1: Select
    p1.optimize([RuleDateFuncReplace])
    p2 = Select(PRODUCT_TABLE, last_sale=Between('2024-01-01', '2024-12-31'))
    return p1 == p2
