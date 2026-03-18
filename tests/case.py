import re
from sql_blocks import detect, Case, If, Range, Pivot, Parser, Schema

"""
Classes that generate CASE in SQL:
    * Case
    * If
    * Range
    * Pivot
"""

GENDER_FIELD = 'gender'
REGEX_BETWEEN = r'WHEN p.age BETWEEN (\d+) AND (\d+) THEN'
REGEX_GENDER = fr"\s+WHEN {GENDER_FIELD} [=] [']([MF])['] THEN ['](.*)[']"

EXPECTED_AGES = [
    ('0', '13'), ('14', '20'), ('21', '40'), ('41', '99')
]
EXPECTED_PTYPES = [
    ('M', 'boy'), ('F', 'girl'),
    ('M', 'young man'), ('F', 'young woman'),
    ('M', 'man'), ('F', 'woman'),
    ('M', 'old man'), ('F', 'old woman')
]


def range_and_if_found() -> dict[str, bool]:
    Parser.public_schema = None
    query = detect('select name from Person p')
    query(
        ptype=Range('age', {
            13: If(GENDER_FIELD, {'M': 'boy',       'F': 'girl'}),
            20: If(GENDER_FIELD, {'M': 'young man', 'F': 'young woman'}),
            40: If(GENDER_FIELD, {'M': 'man',       'F': 'woman'}),
            99: If(GENDER_FIELD, {'M': 'old man',   'F': 'old woman'})
        })
    )
    txt = str(query)
    return {
        'age': re.findall(REGEX_BETWEEN, txt) == EXPECTED_AGES,
        'gender': re.findall(REGEX_GENDER, txt) == EXPECTED_PTYPES,
    }
