# ================================
# 3. Write a decorator `@timing_decorator` to measure and log execution time of any function
# ================================
'''
Decorator:- A decorator is a function that adds extra functionality to another function without changing its original code.
*args :- It allows a function to accept any number of positional arguments as a tuple.
**kwargs :- It allows a function to accept any number of keyword arguments as a dictionary.
@ :- The @ symbol is used to apply a decorator to a function. It is placed above the function definition and followed by the name of the decorator function.

'''
import time

#decorator to mesure execution time
def timing_decorator(func):
 
    def wrapper(*args, **kwargs):

        #record start time
        strat_time = time.time()

        #run the actual function
        result = func(*args, **kwargs)

        #record end time
        end_time = time.time()

        #calculate total time
        execution_time = end_time - strat_time

        print(f"{func.__name__} execute in {execution_time:.4f} seconds")
        return result
    
    return wrapper

'''
# Example usage of the timing_decorator
@timing_decorator
def example_function():
    time.sleep(2)
    print("Function finished")


example_function()
'''

# ================================
#5. Use list/dict comprehensions to extract email domains from customer data and count frequency
# ================================

from typing import List, Dict

def count_email_domains(emails: List[str]) -> Dict[str, int]:
    """
    Extract email domains and count their frequency using comprehensions.
    """

    # extract domains using list comprehension
    domains = [email.split("@")[1] for email in emails]

    # count frequency using dictionary comprehension
    domain_counts = {d: domains.count(d) for d in set(domains)}

    return domain_counts
'''
#example usage

emails = [
    "khushi@gmail.com",
    "rahul@yahoo.com",
    "riya@gmail.com",
    "amit@outlook.com",
    "sneha@hotmail.com",
    "rohit@gmail.com",
    "priya@yahoo.com",
    "vivek@hotmail.com",
    "nidhi@gmail.com"
]

result = count_email_domains(emails)
print(result)
'''

# ===============================================================================================================
# Question 6: Create a context manager for database connections with automatic commit/rollback
# ===============================================================================================================

'''
Working with databases:-
1. connect to database
2. run query
3. commit changes
4. close connections

But problems are:-
1. connection may stay open
2. trasaction may not rollback
3. database may become inconsistent

Context manager:-
Like we use "with open()" which open file, run code and automatically close file, similarly in database connection we will do

so we want our database to:-
1. Open connection
2. Run queries
3. Commit if success
4. Rollback if error
5. Close connection
(all this happen automatically)
'''

import sqlite3  # build-in module for SQLite database. simple, no server required and good for testing
from contextlib import contextmanager  # decorator helps us to create our own context manager
@contextmanager # converts a normal function to context manager (now we can use with db_connection())
def db_connection(db_path: str):  # database path
    
    # conext manager for databaseconnection
    # Automatically commits if successful and rollbacks if error occurs.

    con = sqlite3.connect(db_path) # create connection to database

    try:
        yield con             # give connection to user
        con.commit()          # commit if no error

    except Exception as e:
        con.rollback()        # rollback if error
        print("Transaction failed:", e)

    finally:
        con.close()           # always close connection

'''
#example usage
with db_connection("my_database.db") as conn:
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
'''

# ===============================================================================================================
# Question 7: Implement type hints for all functions and validate using mypy
# ===============================================================================================================

'''
What are type hints?
- Type hints tell python what type of data a function expects and returns.
def add(a , b) -> dont know what type of data will be expected
def add(a:int , b:int) -> int : we are telling that integer will be expected and returned
'''

# ===============================================================================================================
# Question 8: Use `Counter` to find top 10 most frequent pickup locations from taxi data
# ===============================================================================================================

'''
Python contain a powerful tool called collections.counter
It automatically count items
'''
from collections import Counter
from typing import Iterable, List, Tuple

def top_pickup_locations(pu_locations: Iterable[int]) -> List[Tuple[int,int]]:

    location_counts = Counter(pu_locations)
    top_10 = location_counts.most_common(10)
    return top_10

'''
#example usage

import pyarrow.parquet as pq

# read parquet file
table = pq.read_table("D:/Khushi/course/Assignment_2_ Production-Grade ETL Framework_Khushi_Rajvanshi/pyflow-data-pipeline/data/raw/yellow_tripdata_2024-01.parquet")

# convert to column
pu_locations = table.column("PULocationID").to_pylist()

# find top 10 pickup locations
result = top_pickup_locations(pu_locations)

print("Top 10 Pickup Locations:")
for location, count in result:
    print(f"Location {location}: {count} pickups")
'''


# ===============================================================================================================
# Question 9: Use `defaultdict` to group taxi trips by hour of day and calculate average fare
# ===============================================================================================================

from collections import defaultdict  #defaultdict is a subclass where we provide default value for missing keys
from typing import Dict, List        #Dict is type hint for dictionary, list is type hint for list
from datetime import datetime        #datetime is a module to handle date and time in python
import pandas as pd                  


def average_fare_by_hour(trips):

    fare_by_hour = defaultdict(list)  #grouping fares by hour, default value is empty list

    for trip in trips:
        try:
            pickup_time = trip["tpep_pickup_datetime"]
            fare = trip["fare_amount"]  

            # skip invalid fare
            if fare is None:    # filtering data - if fare is none, it will be removed from calculation
                continue

            # handle datetime
            if isinstance(pickup_time, str):    # if pickup_time is string, we need to convert it to datetime object to extract hour
                hour = datetime.strptime(pickup_time, "%Y-%m-%d %H:%M:%S").hour     # to convert string to datetime object
            else:
                hour = pickup_time.hour

            fare_by_hour[hour].append(fare)  # same hours fares will be in one append list

        except Exception as e:
            print(f"Skipping bad record: {e}")

    avg_fare = {}

    for hour, fares in fare_by_hour.items():
        if len(fares) > 0:
            avg_fare[hour] = sum(fares) / len(fares)

    return avg_fare

# Example usage
'''
if __name__ == "__main__":

    file_path = "D:\\Khushi\\course\\Assignment_2_ Production-Grade ETL Framework_Khushi_Rajvanshi\\pyflow-data-pipeline\\data\\raw\\yellow_tripdata_2024-01.parquet"

    # Step 1: Load data
    df = pd.read_parquet(file_path)

    # Step 2: Take small sample (important for performance)
    #df = df.sample(10000, random_state=42)

    #print("Columns:", df.columns)

    # Step 3: Convert to trips
    trips = df.to_dict(orient="records")

    # Step 4: Call function
    result = average_fare_by_hour(trips)

    print("Result length:", len(result))
    for hour in sorted(result):
        print(f"Hour {hour}: Avg Fare = {result[hour]:.2f}₹")
 '''       


# ===============================================================================================================
# Question 10: Use `deque` to implement a rolling data buffer with max 1000 records
# ===============================================================================================================
'''
Rolling buffer (a limited storage):
-> It stores records (like small data entries)
-> It keeps only the latest N records
-> If it becomes full → it automatically removes the oldest record

#deque :- It is a double-ended queue that allows you to add or remove items from both ends efficiently. It is part of the collections module in Python and is ideal for implementing a rolling buffer because it can automatically discard old records when new ones are added beyond its maximum size.
'''

from collections import deque
from typing import Deque, Dict

class RollingBuffer:

    def __init__(self, max_size: int = 1000):  # __init__ is a constructor method that is called when an object of the class is created. It initializes the attributes of the class. max_size is a parameter that sets the maximum size of the buffer, default is 1000.
        self.buffer: Deque[Dict] = deque(maxlen=max_size)  #deque with max length of 1000

    def add_record(self, record: Dict):
        self.buffer.append(record)  #add new record to buffer, if buffer is full, it will automatically remove oldest record

    def get_all(self):
        return list(self.buffer)  #return all records in buffer as a list
    
    def size(self):
        return len(self.buffer)  #return current size of buffer
    
'''
# Example usage
if __name__ == "__main__":

    buffer = RollingBuffer(max_size=5)  # small for testing

    # Add 7 records
    for i in range(7):
        record = {"trip_id": i}
        buffer.add_record(record)
        print(f"Added: {record}, Buffer Size: {buffer.size()}")

    print("\nFinal Buffer:")
    print(buffer.get_all())   
  '''  


# ===============================================================================================================
# Question 12: Design a custom class `TripRecord` with `__eq__`, `__hash__`, `__repr__` for efficient deduplication
# ===============================================================================================================

class TripRecord:

    #Runs automatically when object is created
    def __init__(self, pickup_time, pickup_loc, dropoff_loc):
        self.pickup_time = pickup_time
        self.pickup_loc = pickup_loc
        self.dropoff_loc = dropoff_loc

    #Defines when two objects are equal
    def __eq__(self, other):
        """
        Defines when two objects are equal
        """
        if not isinstance(other, TripRecord):
            return False

        return (
            self.pickup_time == other.pickup_time and
            self.pickup_loc == other.pickup_loc and
            self.dropoff_loc == other.dropoff_loc
        )

    def __hash__(self):
        """
        Makes object hashable (important for set/dict)
        """
        return hash((self.pickup_time, self.pickup_loc, self.dropoff_loc))

    #Defines how object is printed
    def __repr__(self):
        """
        Readable string representation
        """
        return f"TripRecord(time={self.pickup_time}, PU={self.pickup_loc}, DO={self.dropoff_loc})"

'''
if __name__ == "__main__":

    file_path = "D:\\Khushi\\course\\Assignment_2_ Production-Grade ETL Framework_Khushi_Rajvanshi\\pyflow-data-pipeline\\data\\raw\\yellow_tripdata_2024-01.parquet"

    # Step 1: Load your real dataset
    df = pd.read_parquet(file_path).sample(10000, random_state=42)

    # Step 2: Create TripRecord objects (same like trip1, trip2...)
    trips = [
        TripRecord(row["tpep_pickup_datetime"], row["PULocationID"], row["DOLocationID"])
        for _, row in df.iterrows()
    ]

    print("Original Trips (first 5):")
    print(trips[:5],'\n')   # full list huge hogi, isliye first 5

    # Step 3: Remove duplicates using set (same logic)
    unique_trips = set(trips)

    print("\nUnique Trips (first 5):")
    print(list(unique_trips)[:5],'\n')

    print("\nTotal Trips:", len(trips))
    print("Unique Trips:", len(unique_trips))
    print("Duplicates Removed:", len(trips) - len(unique_trips))

'''
# ===============================================================================================================
# Question 13: Read 500MB CSV in chunks of 100K rows without memory overflow
# ===============================================================================================================

def read_csv_in_chunks(file_path: str, chunk_size: int = 100000):

    "Read large CSV file in chunks to avoid memory overflow"
    for chunk in pd.read_csv(file_path, chunksize = chunk_size):

        print(f"Processing chunk with {len(chunk)} rows")

        # Example processing (you can replace this)
        print(chunk.head(2))

'''
if __name__ == "__main__":
    file_path = "D:\\Khushi\\course\\Assignment_2_ Production-Grade ETL Framework_Khushi_Rajvanshi\\pyflow-data-pipeline\\data\\raw\\us-states.csv"
    read_csv_in_chunks(file_path)
'''

# ===============================================================================================================
# Question 14: Auto-detect file encoding and read CSV
# ===============================================================================================================

def read_csv_with_auto_encoding(file_path: str):

    encodings = ["utf-8", "latin1", "cp1252"]

    for enc in encodings:
        try:
            print(f"Trying encoding: {enc}")

            df = pd.read_csv(file_path, encoding=enc)

            print(f"Success with encoding: {enc}")
            return df

        except UnicodeDecodeError:
            print(f"Failed with encoding: {enc}")

    raise ValueError("Could not read file with supported encodings")

'''
# example usage
if __name__ == "__main__":

    file_path = "D:\\Khushi\\course\\Assignment_2_ Production-Grade ETL Framework_Khushi_Rajvanshi\\pyflow-data-pipeline\\data\\raw\\us-states.csv"

    df = read_csv_with_auto_encoding(file_path)

    print("\nData Loaded Successfully!")
    print(df.head())
'''

# ===============================================================================================================
# Question 15: Handle corrupted CSV rows
# ===============================================================================================================

def read_csv_errors(file_path:str, error_file:str):

    bad_row= []

    def bad_line_handler(bad_line):

        bad_row.append(bad_line)
        return None

    df = pd.read_csv(
        file_path, 
        on_bad_lines = bad_line_handler, engine = "python"
    )

    with open(error_file , 'w') as f:
        for i, row in enumerate(bad_row , start=1):
            f.write(f"Line {i} : {row}\n")

    print(f"Total bad rows: {len(bad_row)}")
    print(f"Error log saved to: {error_file}")

    return df

'''
# usage example

if __name__ == "__main__":

    print("\n---- Question 15 ----")

    file_path = "D:\\Khushi\\course\\Assignment_2_ Production-Grade ETL Framework_Khushi_Rajvanshi\\pyflow-data-pipeline\\data\\raw\\us-states.csv"

    error_file = "error_log.txt"

    df = read_csv_errors(file_path, error_file)

    print("\nClean Data:")
    print(df.head())
'''

# ===============================================================================================================
# Question 16: Dynamic Flatten JSON (no predefined columns)
# ===============================================================================================================

def flatten_json_dynamic(data):

    def flatten(record, parent_key="", sep="_"):  # record = current json object , parent_key = key of parent objects , sep = seprator between parent and child keys
        items = {}

        for key, value in record.items():

            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, dict):
                # recursive call for nested dict
                items.update(flatten(value, new_key, sep=sep))

            else:
                items[new_key] = value

        return items

    flat_data = []

    for record in data:
        flat_record = flatten(record)
        flat_data.append(flat_record)

    return flat_data
'''
# usgae example
data = [
    {
        "trip": {
            "id": 1,
            "pickup": {"location": "NY", "time": "10:00"},
            "drop": {"location": "LA", "time": "14:00"}
        }
    },
    {
        "trip": {
            "id": 2,
            "pickup": {"location": "SF", "time": "11:00"},
            "drop": {"location": "TX", "time": "15:00"}
        }
    },
    {
        "trip": {
            "id": 3,
            "pickup": {"location": "CHI", "time": "09:30"},
            "drop": {"location": "NY", "time": "13:30"}
        }
    },
    {
        "trip": {
            "id": 4,
            "pickup": {"location": "DAL", "time": "08:45"},
            "drop": {"location": "SEA", "time": "12:30"}
        }
    },
    {
        "trip": {
            "id": 5,
            "pickup": {"location": "MIA", "time": "07:15"},
            "drop": {"location": "ATL", "time": "10:45"}
        }
    },
    {
        "trip": {
            "id": 6,
            "pickup": {"location": "BOS", "time": "12:00"},
            "drop": {"location": "DC", "time": "16:00"}
        }
    }
]

flattened_data = flatten_json_dynamic(data)
df = pd.DataFrame(flattened_data)
print(df)
'''

# ===============================================================================================================
# Question 18: Read compressed files (gzip, zip) without extracting
# ===============================================================================================================

import pandas as pd
import zipfile

def read_compressed_file(file_path: str):

    # Case 1: GZIP file (.gz)
    if file_path.endswith(".gz"):
        df = pd.read_csv(file_path, compression="gzip")
        return df

    # Case 2: ZIP file (.zip)
    elif file_path.endswith(".zip"):
        with zipfile.ZipFile(file_path, 'r') as z:
            file_names = z.namelist()

            print("Files inside zip:", file_names)

            # Assume first file is CSV
            with z.open(file_names[0]) as f:
                df = pd.read_csv(f)
                return df

    else:
        raise ValueError("Unsupported compressed format")
'''
#usage case    
if __name__ == "__main__":

    print("\n---- Question 18 ----")

    file_path = "../data/raw/us-states.zip"   # or .gz

    df = read_compressed_file(file_path)

    print("Data Preview:")
    print(df.head())
'''

# ===============================================================================================================
# Question 19: Read Parquet file and convert to CSV preserving all data types
# ===============================================================================================================

import pandas as pd

def parquet_to_csv(input_path: str, output_path: str):

    df = pd.read_parquet(input_path)

    print("Parquet loaded successfully!")
    print("columns:" , df.columns)

    df.to_csv(output_path , index = False)

    print(f"CSV saved at: {output_path}")
'''
if __name__ == "__main__":
    print("\n -- Question 19 --")

    input_path = "../data/raw/yellow_tripdata_2024-01.parquet"
    output_path = "../data/raw/yellow_tripdata_2024-01.csv"

    parquet_to_csv(input_path , output_path)

'''

# ===============================================================================================================
# Question 21: Load large CSV with memory optimization
# ===============================================================================================================

import pandas as pd

def load_optimized_csv(file_path: str):

    # int8 -> 1 byte
    # float32 -> 4 bytes
    # Step 1: Define dtypes (VERY IMPORTANT)
    dtypes = {
        "VendorID": "int8",
        "passenger_count": "int8",
        "trip_distance": "float32",
        "RatecodeID": "int8",
        "PULocationID": "int16",
        "DOLocationID": "int16",
        "payment_type": "int8",
        "fare_amount": "float32"
    }

    # Step 2: Read CSV with optimization
    df = pd.read_csv(
        file_path,
        dtype=dtypes,
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    )

    # Step 3: Convert low-cardinality columns to category
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("category")

    return df
'''
if __name__ == "__main__":

    print("\n---- Optimized CSV Loading ----")

    file_path = "../data/raw/yellow_tripdata_2024-01.csv"

    df = load_optimized_csv(file_path)

    print(df.info())  # 🔥 shows memory usage
'''
# --------------------------------------------------
# Question 22 - Handle missing values intelligently: 
#    - Numeric: mean/median based on distribution 
#    - Categorical: mode or 'Unknown' 
#    - Time: forward-fill for time series
# --------------------------------------------------
# ===============================================================================================================
# Question 22: Handle missing values intelligently
# ===============================================================================================================

import pandas as pd

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame: 

    # 🔹 Numeric columns
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns # find all numeric columns

    for col in numeric_cols:
        if df[col].isnull().sum() > 0:

            # Check skewness
            if df[col].skew() > 1:
                # skewed → median
                df[col].fillna(df[col].median(), inplace=True)
            else:
                # normal → mean
                df[col].fillna(df[col].mean(), inplace=True)

    # 🔹 Categorical columns
    cat_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in cat_cols:
        if df[col].isnull().sum() > 0:
            mode = df[col].mode()
            if not mode.empty:
                df[col].fillna(mode[0], inplace=True)
            else:
                df[col].fillna("Unknown", inplace=True)

    # 🔹 Time columns
    time_cols = df.select_dtypes(include=["datetime64[ns]"]).columns

    for col in time_cols:
        df[col] = df[col].ffill()

    return df

# Example usage

if __name__ == "__main__":

    print("\n---- Question 22 ----")

    file_path = "../data/raw/yellow_tripdata_2024-01.parquet"

    df = pd.read_parquet(file_path).head(10000)

    print("Before cleaning:")
    print(df.isnull().sum())

    df = handle_missing_values(df)

    print("\nAfter cleaning:")
    print(df.isnull().sum())

    #test comment
    