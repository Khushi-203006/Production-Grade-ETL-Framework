# --------------------------------------------------
# Question 23: Detect and handle outliers using IQR method for fare amounts
# --------------------------------------------------
print("Running file...")
import pandas as pd

def handle_outliner_iqr(df: pd.DataFrame, column: str) -> pd.DataFrame:

    #step1: calculate Q1 and Q3
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)

    #step2: calculate IQR
    IQR = Q3 - Q1

    #step3: calculate lower and upper bounds
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    #step4: filter out outliers
    cleaned_df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

    return cleaned_df

#usage example
if __name__ == "__main__":

    data = {
        "fare_amount": [100, 120, 130, 150, 200, 5000]
    }

    df = pd.DataFrame(data)

    cleaned_df = handle_outliner_iqr(df, "fare_amount")

    print("Original Data:\n", df)
    print("\nCleaned Data:\n", cleaned_df)