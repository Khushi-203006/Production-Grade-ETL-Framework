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