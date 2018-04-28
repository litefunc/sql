import pandas as pd
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
from typing import List, Iterable
from sql.utils import quote_join


class Insert():
    def __init__(self, table: str, cols: Iterable):
        __columns = quote_join('"', cols)
        __values = ', '.join([f'${i+1}' for i in range(len(cols))])
        self.sql = f'INSERT INTO "{table}"({__columns}) VALUES({__values})'

    def run(self, conn, df, commit=False):
        print(self.sql)
        cur = conn.cursor()
        li = df.values.tolist()
        for row in li:
            cur.execute(self.sql, tuple(row))
        if commit: 
            conn.commit()
        
        
# insert into
def i(table: str, cols: Iterable) -> Insert:
    return Insert(table, cols)


