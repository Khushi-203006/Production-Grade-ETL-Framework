# --------------------------------------------------
# Question 23: Detect and handle outliers using IQR method for fare amounts
# --------------------------------------------------

import pandas as pd

def handle_outliner_iqr(df: pd.DataFrame, column: str) -> pd.DataFrame: 

    #step1: calculate Q1 and Q3
    Q1 = df[column].quantile(0.25) #data which is less than 25% of the data
    Q3 = df[column].quantile(0.75) #data which is less than 75% of the data

    #step2: calculate IQR
    IQR = Q3 - Q1   #interquartile range is the difference between Q3 and Q1

    #step3: calculate lower and upper bounds
    # Outliers are typically defined as data points that fall below Q1 - 1.5*IQR or above Q3 + 1.5*IQR
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    #step4: filter out outliers
    # We keep only the data points that are within the lower and upper bounds
    cleaned_df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

    return cleaned_df
'''
#usage example
if __name__ == "__main__":

    data = {
        "fare_amount": [100, 120, 130, 150, 200, 5000]
    }

    df = pd.DataFrame(data)

    cleaned_df = handle_outliner_iqr(df, "fare_amount")

    print("Original Data:\n", df)
    print("\nCleaned Data:\n", cleaned_df)
'''

# --------------------------------------------------
# Question 24: Remove duplicates based on business keys (trip_start_time + pickup_location + 
#              dropoff_location)
# --------------------------------------------------

import pandas as pd

def remove_duplicates(
    df: pd.DataFrame,
    subset_cols: list
) -> pd.DataFrame:
    """
    Remove duplicate records based on business keys.

    Args:
        df: Input DataFrame
        subset_cols: Columns to consider for duplicate detection

    Returns:
        DataFrame with duplicates removed
    """

    # Step 1: Count duplicates (for logging/debugging)
    before = len(df)

    # Step 2: Remove duplicates
    df_cleaned = df.drop_duplicates(subset=subset_cols, keep="first")

    # Step 3: Count after cleaning
    after = len(df_cleaned)

    print(f"Removed {before - after} duplicate rows")

    return df_cleaned
'''
if __name__ == "__main__":

    data = {
        "trip_start_time": ["10:00", "10:00", "11:00"],
        "pickup_location": ["A", "A", "A"],
        "dropoff_location": ["B", "B", "C"],
        "fare_amount": [100, 100, 150]
    }

    df = pd.DataFrame(data)

    cleaned_df = remove_duplicates(
        df,
        ["trip_start_time", "pickup_location", "dropoff_location"]
    )

    print("\nOriginal Data:\n", df)
    print("\nCleaned Data:\n", cleaned_df)
'''

# --------------------------------------------------
# Question 25: Merge 3 DataFrames (trips, weather, holidays) using different join types, validate merge 
#             quality 
# --------------------------------------------------

import pandas as pd

def merge_datasets(
    trips_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    holidays_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge trips, weather, and holidays data.

    Returns:
        Merged DataFrame
    """

    # Step 1: Merge trips + weather
    merged_df = pd.merge(
        trips_df,
        weather_df,
        on="date",
        how="left"
    )

    # Step 2: Merge with holidays
    merged_df = pd.merge(
        merged_df,
        holidays_df,
        on="date",
        how="left"
    )

    return merged_df

def validate_merge(df: pd.DataFrame):
    
    print("\n🔍 Merge Validation Report")

    # Missing values check
    print("\nMissing Values:")
    print(df.isnull().sum())

    # Total rows
    print("\nTotal Rows:", len(df))
'''
if __name__ == "__main__":  
    trips = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "trips": [1000, 1200]
    })

    weather = pd.DataFrame({
        "date": ["2024-01-01"],
        "weather": ["Rainy"]
    })

    holidays = pd.DataFrame({
        "date": ["2024-01-01"],
        "holiday": ["New Year"]
    })

    merged = merge_datasets(trips, weather, holidays)

    print("\nMerged Data:\n", merged)

    validate_merge(merged)
 '''

# --------------------------------------------------
# Question 26: Group by multiple columns & aggregate with custom functions
# --------------------------------------------------

import pandas as pd

def group_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group data by date, hour, borough and perform aggregations.
    
    Returns:
        Aggregated DataFrame
    """

    grouped_df = df.groupby(
        ["date", "hour", "borough"]
    ).agg(
        total_fare=("fare_amount", "sum"),
        avg_fare=("fare_amount", "mean"),
        trip_count=("fare_amount", "count")
    ).reset_index()

    return grouped_df
'''
if __name__ == "__main__":

    data = {
        "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
        "hour": [10, 10, 11],
        "borough": ["A", "A", "B"],
        "fare_amount": [100, 150, 200]
    }

    df = pd.DataFrame(data)

    result = group_and_aggregate(df)

    print(result)
'''
# --------------------------------------------------
# Question 27:  Handle date/time: parse multiple formats, extract features (hour, day_of_week, is_weekend, 
#             is_holiday) 
# --------------------------------------------------

import pandas as pd

def process_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Parse datetime column and extract useful features.

    Args:
        df: Input DataFrame
        column: Date column name

    Returns:
        DataFrame with new datetime features
    """

    # Step 1: Convert to datetime (handles multiple formats)
    df[column] = pd.to_datetime(df[column], errors="coerce")

    # Step 2: Extract features
    df["hour"] = df[column].dt.hour
    df["day_of_week"] = df[column].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6])

    return df
'''
if __name__ == "__main__":

    data = {
        "date": ["2024-01-01 10:30:00", "2024-01-02", "March 3, 2024"]
    }

    df = pd.DataFrame(data)

    df = process_datetime(df, "date")

    print(df)
'''    

# --------------------------------------------------
# Question 28:  7-day Rolling Average of Trip Count by Location
# --------------------------------------------------
import pandas as pd

def rolling_avg_trip_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate 7-day rolling average of trip count by location (borough).
    """

    # Step 1: Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Step 2: Sort data (VERY IMPORTANT for rolling)
    df = df.sort_values(by=["borough", "date"])

    # Step 3: Apply rolling window
    df["rolling_avg"] = (
        df.groupby("borough")["trip_count"]
        .rolling(window=7, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    return df
'''
# usage example
if __name__ == "__main__":

    data = {
        "date": [
            "2024-01-01", "2024-01-02", "2024-01-03",
            "2024-01-04", "2024-01-05", "2024-01-06",
            "2024-01-07"
        ],
        "borough": ["A", "A", "A", "A", "A", "A", "A"],
        "trip_count": [100, 120, 130, 140, 150, 160, 170]
    }

    df = pd.DataFrame(data)

    result = rolling_avg_trip_count(df)

    print("\nFinal Output:\n", result)
'''

# --------------------------------------------------
# Question 29: Complex Filtering using .query()
# --------------------------------------------------
import pandas as pd

def filter_rush_hour_trips(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter trips during rush hours on weekdays with fare > 20.
    """

    # Ensure datetime
    df["date"] = pd.to_datetime(df["date"])

    # Extract required columns
    df["hour"] = df["date"].dt.hour
    df["day_of_week"] = df["date"].dt.dayofweek  # 0=Mon, 6=Sun

    # Apply query
    filtered_df = df.query(
        "(hour >= 7 and hour <= 10 or hour >= 17 and hour <= 20) "
        "and day_of_week < 5 "
        "and fare_amount > 20"
    )

    return filtered_df

'''
# usage example
if __name__ == "__main__":

    data = {
        "date": [
            "2024-01-01 08:00:00",  # weekday + rush ✔
            "2024-01-01 14:00:00",  # not rush ❌
            "2024-01-06 09:00:00",  # weekend ❌
            "2024-01-02 18:00:00",  # rush ✔
        ],
        "fare_amount": [50, 30, 100, 10]  # last one low fare ❌
    }

    df = pd.DataFrame(data)

    result = filter_rush_hour_trips(df)

    print("\nFiltered Data:\n", result)
'''    

# --------------------------------------------------
# Q30: Export Data to Multiple Formats
# --------------------------------------------------
import pandas as pd
import os

def export_data(df: pd.DataFrame, output_dir: str = "output"):
    """
    Export DataFrame to CSV, JSON, Parquet, and Excel with compression.
    """

    # Step 1: Create output directory if not exists
    os.makedirs(output_dir, exist_ok=True)

    # Step 2: Export CSV (compressed)
    df.to_csv(f"{output_dir}/data.csv.gz", index=False, compression="gzip")

    # Step 3: Export JSON (compressed)
    df.to_json(f"{output_dir}/data.json.gz", orient="records", lines=True, compression="gzip")

    # Step 4: Export Parquet (efficient format)
    df.to_parquet(f"{output_dir}/data.parquet", index=False)

    # Step 5: Export Excel
    df.to_excel(f"{output_dir}/data.xlsx", index=False)

    print("\n✅ Data exported successfully to all formats!")


# usage example
if __name__ == "__main__":

    data = {
        "date": ["2024-01-01", "2024-01-02"],
        "borough": ["A", "B"],
        "trip_count": [100, 200],
        "fare_amount": [50, 80]
    }

    df = pd.DataFrame(data)

    export_data(df)