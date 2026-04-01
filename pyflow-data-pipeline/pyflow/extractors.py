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
