import sqlite3
import settings

logger = settings.logging.getLogger()

class Table:
    def __init__(self):
        self.columns = ["ticker", "interval", "exchange", "measure_name", "measure_value", "time"]

    @staticmethod
    def aggregate_insert_values(ticker):
        values = []
        for measure in ticker.measures:
            values.append((ticker.coin+"_"+ticker.currency, ticker.interval, ticker.exchange, measure, ticker.measures[measure], ticker.time))
        return values


    def insert(self, ticker):
        connection = sqlite3.connect(settings.SQLITE_DATABASE)
        cls = self.__class__

        values = cls.aggregate_insert_values(ticker)
        cursor = connection.cursor()
        query = f"""INSERT INTO {settings.SQLITE_TABLE} VALUES (?,?,?,?,?,?);"""     
        try:
            cursor.executemany(query, values)
            connection.commit()
        except Exception as e:
            logger.error(f"Error on sqlite insertion: {e}")
        
        cursor.close()