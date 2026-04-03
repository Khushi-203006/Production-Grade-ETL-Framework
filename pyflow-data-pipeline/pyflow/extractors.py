import csv #can read, write and manipulate csv files
from email import generator 
from typing import Generator, Dict

'''
4. Implement a generator function to read 500MB CSV file line-by-line without loading entire file 
in memory

Instead of loading entire file, we will readfile line by line using a generator function.
This generrator will yeild one line at a time and will pause function for a while.
We will use 'yield' keyword for this purpose. 
'''

def read_large_csv(file_path: str) -> Generator[Dict, None, None]:
    """
    Generator function to read a large CSV file row by row.
    This prevents loading the entire file into memory.
    file_path: str - the path to the csv where the data is in string format as input
    return : generator that yeilds each row as a dictionary where keys are column names and values are corresponding data for that row.
    """

    try:
        with open(file_path, mode="r", encoding="utf-8") as file:

            #convert each row into a dictionary 
            reader = csv.DictReader(file)

            for row in reader:
                yield row

    except Exception as e:
        print(f"Error reading file: {e}")

'''
file_path = "data/raw/gdp.csv"
generator = read_large_csv(file_path)
for i , row in enumerate(generator):
    print(row)
    if i == 5:
        break       
'''

# --------------------------------------------------
# Question 31 : Extract Module: Create `extractor.py` with classes for CSVExtractor, JSONExtractor, 
# ParquetExtractor (inheritance/polymorphism) 
# --------------------------------------------------
import pandas as pd
from abc import ABC, abstractmethod

# Base Extractor Class
class BaseExtractor(ABC):
    """
    Abstract base class for all extractors.
    """

    @abstractmethod
    def extract(self, file_path: str) -> pd.DataFrame:
        pass

# CSV Extractor
class CSVExtractor(BaseExtractor):

    def extract(self, file_path: str) -> pd.DataFrame:
        print(f"📂 Reading CSV file: {file_path}")
        return pd.read_csv(file_path)


# JSON Extractor
class JSONExtractor(BaseExtractor):

    def extract(self, file_path: str) -> pd.DataFrame:
        print(f"📂 Reading JSON file: {file_path}")
        return pd.read_json(file_path)


# Parquet Extractor
class ParquetExtractor(BaseExtractor):

    def extract(self, file_path: str) -> pd.DataFrame:
        print(f"📂 Reading Parquet file: {file_path}")
        return pd.read_parquet(file_path)

'''
# usage example
if __name__ == "__main__":

    # Sample test (use your own files if available)
    
    # CSV Example
    csv_extractor = CSVExtractor()
    try:
        df_csv = csv_extractor.extract("pyflow-data-pipeline/data/raw/yellow_tripdata_2024-01.csv")
        print("\nCSV Data:\n", df_csv.head())
    except Exception as e:
        print("CSV Error:", e)

    # JSON Example
    json_extractor = JSONExtractor()
    try:
        df_json = json_extractor.extract("D:\\Khushi\\course\\python\\Mini Project 1 convert csv file to json file\\retail_db_json\\categories\\part-00000.json")
        print("\nJSON Data:\n", df_json.head())
    except Exception as e:
        print("JSON Error:", e)

    # Parquet Example
    parquet_extractor = ParquetExtractor()
    try:
        df_parquet = parquet_extractor.extract("D:\\Khushi\\course\\Assignment_2_ Production-Grade ETL Framework_Khushi_Rajvanshi\\pyflow_ETL\\Production-Grade-ETL-Framework\\pyflow-data-pipeline\\data\\raw\\yellow_tripdata_2024-01.parquet")
        print("\nParquet Data:\n", df_parquet.head())
    except Exception as e:
        print("Parquet Error:", e)
'''