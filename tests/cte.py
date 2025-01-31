import re
from difflib import SequenceMatcher
from sql_blocks.sql_blocks import *


def compare_recursive_text(obj: Recursive) -> bool:
    txt1 = re.sub(r'\s+', ' ', str(obj) ).lower()
    txt2 = re.sub(r'\s+', ' ', """
        WITH RECURSIVE ancestors AS (
                SELECT f1.id, f1.name, f1.father, f1.mother, f1.birth
                FROM Folks f1 WHERE f1.id = 32630
        UNION ALL
                SELECT f2.id, f2.name, f2.father, f2.mother, f2.birth
                FROM Folks f2 , ancestors a WHERE (f2.id = a.father OR f2.id = a.mother)
        )SELECT * FROM ancestors
    """).lower()
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66

def recursive_obj():
    def get_query(i: int):
        query = Select(f"Folks f{i}")
        query.add_fields('id,name,father,mother,birth')
        return query
    q1, q2 = [get_query(i) for i in (1, 2)]
    q1(id=eq(32630))
    q2(
        id=Where.formula('({af} = a.father OR {af} = a.mother)')
    )
    return Recursive('ancestors a', [q1, q2])
