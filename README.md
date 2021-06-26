1. cp. .env.example .env
2. python -m venv venv
3. pip install -r requirements.txt
4. Create coinmove.db file in root dir
5. sqlite3 coinmove.db
6. CREATE TABLE technical_data (ticker TEXT, interval TEXT, exchange TEXT, measure_name TEXT, measure_value REAL, time);