import sqlite3
from typing import Dict, List
import settings

class Ticker:
    def __init__(self, ticker:str="BTC_USDT"):
        self.ticker = ticker
        self.columns = ["ticker", "interval", "exchange", "measure_name", "measure_value", "time"]
        self.conn = self.create_connection()

    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(settings.SQLITE_DATABASE)
        except Exception as e:
            print(e)

        return conn

    def get_measures(self)->Dict[str, list]:
        rows = self.get_rows(limit=21)
        result = {}
        for row in rows:
            measure_name = row[self.columns.index("measure_name")]
            if measure_name not in result:
                result[measure_name] = []

            if "time" not in result:
                result["time"] = []
            
            record_time = row[self.columns.index("time")]
            if record_time not in result["time"]:
                result["time"].append(record_time)

            result[measure_name].append(row[self.columns.index("measure_value")])
        
        return result


    def get_rows(self, limit:int)->list:
        """
        If the retrieving measures are (high, low, close) and the limit is 2, 
        then you will get with 6 rows with two different times.
        """
        measures = ('high','low','close','volume')
        scan_window = len(measures) * limit
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {settings.SQLITE_TABLE} WHERE ticker = '{self.ticker}' AND measure_name IN {measures} ORDER BY time DESC LIMIT {scan_window};")

        rows = cur.fetchall()

        return rows


def serialize_tickers(tickers:List[str]):
    combined_and_serialized_tickers = {}
    for ticker in tickers:
        ticker_obj = Ticker(ticker)
        combined_and_serialized_tickers[ticker] = ticker_obj.get_measures()

    return combined_and_serialized_tickers