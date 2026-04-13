import re
from sql_blocks import detect, Insert
from tests.util import (
    create_public_schema, 
    remove_public_schema
)
from difflib import SequenceMatcher


def insert_from_query() -> Insert:
    query = detect("SELECT quantity, cus_id, pro_id FROM Sales WHERE ref_date > '2025-12-24' ")
    return Insert(query)

def insert_from_dict() -> Insert:
    return Insert({
        'serial_number': 'A57B48C66',
        'name': 'Laptop', 'price': 999.99
    })

def insert_from_list() -> Insert:
    return Insert([
        ('111.111.11-11', "John Doe",       1),
        ('222.222.22-22', "Jane Smith",     2),
        ('333.333.33-33', "David Lee",      2),
        ('444.444.44-44', 'Sarah Connor',   1),
        ('555.555.55-55', 'Laura Palmer',   4),
    ], table_name='Customer') # --- must have table_name !

def text_insert_from_query() -> str:
    return """INSERT INTO Sales (quantity, cus_id, pro_id)
    SELECT
            sal.quantity,
            sal.cus_id,
            sal.pro_id
    FROM
            Sales sal
    WHERE
            ref_date > '2025-12-24';"""

def text_insert_from_dict() -> str:
    return """INSERT INTO Product (serial_number, name, price) VALUES
            ('A57B48C66', 'Laptop', 999.99);"""

def text_insert_from_list() -> str:
    return """INSERT INTO Customer (driver_licence, name, region) VALUES
            ('111.111.11-11', 'John Doe', 1),
            ('222.222.22-22', 'Jane Smith', 2),
            ('333.333.33-33', 'David Lee', 2),
            ('444.444.44-44', 'Sarah Connor', 1),
            ('555.555.55-55', 'Laura Palmer', 4);"""

def remove_spaces(txt: str) -> str:
    return re.sub(r'\s+', ' ', txt)

def compare_insert_from_dict() -> bool:
    create_public_schema()
    txt1 = remove_spaces( str(insert_from_dict()) )
    txt2 = remove_spaces( text_insert_from_dict() )
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66

def compare_insert_from_query() -> bool:
    remove_public_schema()
    txt1 = remove_spaces( str(insert_from_query()) )
    txt2 = remove_spaces( text_insert_from_query() )
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66

def compare_insert_from_list() -> bool:
    create_public_schema()
    txt1 = remove_spaces( str(insert_from_list()) )
    txt2 = remove_spaces( text_insert_from_list() )
    return SequenceMatcher(None, txt1, txt2).ratio() > 0.66
