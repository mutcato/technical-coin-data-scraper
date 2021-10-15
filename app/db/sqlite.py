import sqlite3
import settings

logger = settings.logging.getLogger()

class Table:
    TTL = {
        "5m": 2 * 24 * 60 * 60,
        "15m": 6 * 24 * 60 * 60,
        "1h": 24 * 24 * 60 * 60,
        "4h": 96 * 24 * 60 * 60,
        "8h": 192 * 24 * 60 * 60,
        "1d": 360 * 24 * 60 * 60,
    }

    def __init__(self):
        self.columns = ["ticker", "interval", "exchange", "measure_name", "measure_value", "time", "ttl"]

    def aggregate_insert_values(self, tickers):
        values = []
        for ticker in tickers:
            for measure in ticker.measures:
                values.append((
                    ticker.coin+"_"+ticker.currency, 
                    ticker.interval, 
                    ticker.exchange, 
                    measure, 
                    ticker.measures[measure], 
                    ticker.time,
                    ticker.time + self.TTL[ticker.interval]))
        return values


    def batch_insert(self, tickers):
        connection = sqlite3.connect(settings.SQLITE_DATABASE)
        values = self.aggregate_insert_values(tickers)
        cursor = connection.cursor()
        query = f"""INSERT INTO {settings.SQLITE_TABLE} VALUES (?,?,?,?,?,?,?);"""     
        try:
            cursor.executemany(query, values)
            connection.commit()
        except Exception as e:
            logger.error(f"Error on sqlite insertion: {e}")
        
        cursor.close()
