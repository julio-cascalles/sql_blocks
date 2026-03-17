from sys import argv
from sql_blocks import execute


try:
    print(
        execute(argv) or ''
    )
except:
    print('\t Failed to load scripts. Check the file encoding.')