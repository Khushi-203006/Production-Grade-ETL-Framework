# --------------------------------------------------
# Question 33:  Create `loader.py` to load data to PostgreSQL/MySQL with batch inserts 
# (1000 rows/batch) 
# --------------------------------------------------

import pandas as pd
import sqlite3



# Base Loader Class
class BaseLoader:
    def load(self, df: pd.DataFrame, table_name: str):
        raise NotImplementedError

# SQLite Loader
class SQLiteLoader(BaseLoader):

    def __init__(self, db_path: str = "my_database.db"):
        self.db_path = db_path

    def load(self, df: pd.DataFrame, table_name: str):
        print(f" Loading data into table: {table_name}")

        conn = sqlite3.connect(self.db_path)

        # Insert data
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",   # replace or append
            index=False
        )

        conn.close()

        print("Data loaded successfully!")

# usage example
if __name__ == "__main__":

    data = {
        "date": ["2024-01-01", "2024-01-02"],
        "borough": ["A", "B"],
        "trip_count": [100, 200],
        "fare_amount": [50, 80]
    }

    df = pd.DataFrame(data)

    loader = SQLiteLoader("test.db")

    loader.load(df, "trips")

    print("\n Data saved in test.db (table: trips)")

