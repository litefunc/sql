from typing import List, Iterable
from cytoolz.dicttoolz import itemmap

def quote(symbol: str, it: Iterable) -> List[str]:
    return [f'{symbol}{col}{symbol}' for col in it]


def quote_join(symbol: str, it: Iterable) -> str:
    return ', '.join(quote(symbol, it))
