import pandas as pd
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
from typing import List, Iterable
from sql.utils import quote_join


class Exce():
    def __init__(self, sql: str, values: Iterable=()):
         self.sql = sql
         self.values = values

    def where(self, condition: dict):
        __condition = ' and '.join([f'"{key}"=%s' for key in condition.keys()])
        return Exce(f'{self.sql} WHERE {__condition}', self.values + tuple(condition.values()))

    def run(self, conn) -> None:
        print(self.sql)
        cur = conn.cursor()
        cur.execute(self.sql, self.values)


class Update(Exce):
    def __init__(self, table: str, d: dict):
        __columns = ' and '.join([f'"{key}"=%s' for key in d.keys()])
        self.sql = f'UPDATE "{table}" SET {__columns}'
        self.values = tuple(d.values())