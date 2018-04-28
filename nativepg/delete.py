import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
from typing import List, Iterable
from sql.utils import quote_join


class Exce():
    def __init__(self, sql: str, values: Iterable):
         self.sql = sql
         self.values = values

    def where(self, condition: dict):
        __condition = ' and '.join([f'"{v}"=${i+1}' for i, v in enumerate(condition.keys())])
        return Exce(f'{self.sql} WHERE {__condition}', tuple(condition.values()))

    def run(self, conn):
        print(self.sql)
        cur = conn.cursor()
        cur.execute(self.sql, self.values)


class Delete(Exce):
    def __init__(self, table: str):
        self.sql = f'DELETE  FROM "{table}"'
        self.values = None
