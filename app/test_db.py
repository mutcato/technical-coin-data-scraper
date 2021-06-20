import settings
from db import sqlite


def test_insert_sqlite():
    data = {
        "e": "kline",                           # event type
        "E": 1499404907056,                     # event time
        "s": "ETHBTC",                          # symbol
        "k": {
            "t": 1499404860000,                 # start time of this bar
            "T": 1499404919999,                 # end time of this bar
            "s": "ETHBTC",                      # symbol
            "i": "1m",                          # interval
            "f": 77462,                         # first trade id
            "L": 77465,                         # last trade id
            "o": "0.10278577",                  # open
            "c": "0.10278645",                  # close
            "h": "0.10278712",                  # high
            "l": "0.10278518",                  # low
            "v": "17.47929838",                 # volume
            "n": 4,                             # number of trades
            "x": False,                         # whether this bar is final
            "q": "1.79662878",                  # quote volume
            "V": "2.34879839",                  # volume of active buy
            "Q": "0.24142166",                  # quote volume of active buy
            "B": "13279784.01349473"    # can be ignored
        }
    }

    table = sqlite.Table()
    table.insert()


    assert True
    