import pandas as pd
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
from typing import List, Iterable
from sql.utils import quote_join


class Query():
    def __init__(self, sql: str, values: Iterable):
         self.sql = sql
         self.values = values

    def where(self, condition: dict):
        __condition = ' and '.join([f'"{v}"=${i+1}' for i, v in enumerate(condition.keys())])
        return Query(f'{self.sql} WHERE {__condition}', tuple(condition.values()))

    def orderby(self, orders: Iterable[str]):
        __orders = quote_join('"', orders)
        return Query(f'{self.sql} ORDER BY {__orders}', self.values)

    def limit(self, n: int):
        return Query(f'{self.sql} LIMIT {n}', self.values)

    def list(self, conn) -> List:
        print(self.sql)
        cur = conn.cursor()
        cur.execute(self.sql, self.values)
        return cur.fetchall()

    def df(self, conn):
        print(self.sql)
        return pd.read_sql_query(self.sql, conn, params=self.values)


class Select(Query):
    def __init__(self, cols: Iterable, table: str):
        __columns = quote_join('"', cols)
        self.cols = cols
        self.sql = f'SELECT {__columns} FROM "{table}"'
        self.values = None


class SelectAll(Query):
    def __init__(self, table: str):
        self.sql = f'SELECT * FROM "{table}"'
        self.values = None


class SelectDistict(Query):
    def __init__(self, cols: Iterable, table: str):
        __columns = quote_join('"', cols)
        self.sql = f'SELECT DISTINCT {__columns} FROM "{table}"'
        self.cols = cols
        self.values = None       


# select
def s(cols: Iterable, table: str) -> Select:
    return Select(cols, table)


# select where
def sw(cols: Iterable, table: str, where: dict) -> Select:
    return Select(cols, table).where(where)


# select order by
def so(cols: Iterable, table: str, orderby: Iterable) -> Select:
    return Select(cols, table).orderby(orderby)


# select where order by
def swo(cols: Iterable, table: str, where: dict, orderby: Iterable) -> Select:
    return Select(cols, table).where(where).orderby(orderby)


# select all
def sa(table: str) -> SelectAll:
    return SelectAll(table)


# select all where
def saw(table: str, where: dict) -> SelectAll:
    return SelectAll(table).where(where)


# select all order by
def sao(table: str, orderby: Iterable) -> SelectAll:
    return SelectAll(table).orderby(orderby)


# select all where order by
def sawo(table: str, where: dict, orderby: Iterable) -> SelectAll:
    return SelectAll(table).where(where).orderby(orderby)


# select distinct
def sd(cols: Iterable, table: str) -> SelectDistict:
    return SelectDistict(cols, table)


# select distinct where
def sdw(cols: Iterable, table: str, where: dict) -> SelectDistict:
    return SelectDistict(cols, table).where(where)


# select distinct order by
def sdo(cols: Iterable, table: str, orderby: Iterable) -> SelectDistict:
    return SelectDistict(cols, table).orderby(orderby)


# select distinct where order by
def sdwo(cols: Iterable, table: str, where: dict, orderby: Iterable) -> SelectDistict:
    return SelectDistict(cols, table).where(where).orderby(orderby)