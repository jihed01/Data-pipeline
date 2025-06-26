# data_loader.py
import os
import pandas as pd
from pathlib import Path


def load_visitor_data(raw_data_path: str = "data/raw") -> pd.DataFrame:
    """
    Load all visitor CSV files from the raw data directory and combine them into a single DataFrame.

    Args:
        raw_data_path: Path to the directory containing the raw CSV files

    Returns:
        pd.DataFrame: Combined visitor data from all months
    """
    # Create a Path object for the raw data directory
    raw_dir = Path(raw_data_path)

    # Verify the directory exists
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")

    # Find all CSV files in the directory
    csv_files = list(raw_dir.glob("visiteurs_*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No visitor CSV files found in {raw_dir}")

    # Read and concatenate all CSV files
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
            print(f"Loaded data from {file.name}")
        except Exception as e:
            print(f"Error loading {file.name}: {str(e)}")

    if not dfs:
        raise ValueError("No valid data could be loaded from the CSV files")

    combined_df = pd.concat(dfs, ignore_index=True)

    # Convert date column to datetime
    combined_df['date'] = pd.to_datetime(combined_df['date'])

    # Sort by date and sensor
    combined_df.sort_values(['date', 'id_du_capteur', 'id_du_magasin'], inplace=True)

    return combined_df


if __name__ == "__main__":
    try:
        # Load and display the data
        visitor_data = load_visitor_data()

        print("\nCombined Visitor Data:")
        print(visitor_data)

        # Show basic info about the data
        print("\nData Info:")
        print(visitor_data.info())

        # Show summary statistics
        print("\nSummary Statistics:")
        print(visitor_data.describe())

    except Exception as e:
        print(f"Error: {str(e)}")