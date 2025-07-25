import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os


def extract_sleep_data(zip_path, output_file="sleep_data.parquet"):
    """
    Extract sleep data from Apple Health export.zip for the last 30 days of available data.

    Args:
        zip_path (str): Path to the export.zip file
        output_file (str): Output file path (default: sleep_data.parquet)
    """

    # All sleep-related data types in Apple Health
    sleep_keywords = ['sleep', 'Sleep', 'SLEEP']

    sleep_records = []

    # Extract and parse XML from zip
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        with zip_ref.open('apple_health_export/export.xml') as xml_file:
            # Parse XML iteratively to handle large files efficiently
            context = ET.iterparse(xml_file, events=('start', 'end'))
            context = iter(context)
            event, root = next(context)

            for event, elem in context:
                if event == 'end' and elem.tag == 'Record':
                    record_type = elem.get('type')

                    # Check if it's sleep-related data (any type containing 'sleep')
                    if record_type and any(keyword in record_type for keyword in sleep_keywords):
                        record = {
                            'type': record_type,
                            'start_date': elem.get('startDate'),
                            'end_date': elem.get('endDate'),
                            'value': elem.get('value'),
                            'unit': elem.get('unit'),
                            'source_name': elem.get('sourceName'),
                            'creation_date': elem.get('creationDate')
                        }
                        sleep_records.append(record)

                    # Clear element to save memory
                    elem.clear()
                    root.clear()

    if not sleep_records:
        print("No sleep data found in the export.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(sleep_records)

    # Convert date columns to datetime (Apple Health uses ISO format)
    date_columns = ['start_date', 'end_date', 'creation_date']
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], format='ISO8601', utc=True)
            except:
                # Fallback to automatic parsing if ISO format fails
                df[col] = pd.to_datetime(df[col], errors='coerce')

    # Find the latest date in the data and get last 30 days
    if 'start_date' in df.columns:
        latest_date = df['start_date'].max()
        cutoff_date = latest_date - timedelta(days=30)

        # Filter for last 30 days
        df = df[df['start_date'] >= cutoff_date]

    if df.empty:
        print("No sleep data found in the last 30 days.")
        return

    # Save to parquet (most efficient for querying)
    df.to_parquet(output_file, index=False)

    print(f"Extracted {len(df)} sleep records from the last 30 days")
    print(f"Data saved to: {output_file}")
    print(f"Date range: {df['start_date'].min()} to {df['start_date'].max()}")

    return df


# Usage
if __name__ == "__main__":
    # Replace with your export.zip path
    zip_path = "export.zip"

    if os.path.exists(zip_path):
        df = extract_sleep_data(zip_path)

        # Optional: display sample data
        if df is not None and not df.empty:
            print("\nSample data:")
            print(df.head())
    else:
        print(f"File not found: {zip_path}")
        print("Please ensure export.zip is in the same directory or provide the full path.")
