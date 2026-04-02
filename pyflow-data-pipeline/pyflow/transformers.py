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