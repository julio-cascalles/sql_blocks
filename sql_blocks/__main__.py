from sys import argv
from sql_blocks import execute


print(
    execute(argv) or ''
)