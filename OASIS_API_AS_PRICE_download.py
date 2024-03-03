import requests
import datetime
import zipfile
import csv
import io
import pandas as pd
import time

start_year = 2023
start_month = 5
end_year = 2023
end_month = 6

output_path = "dataoutput.csv"
base_url = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&"

params = {
    "queryname": "PRC_AS",
    "version": "12",
    "startdatetime": None,
    "enddatetime": None,
    "market_run_id": "DAM",
    #"anc_type": "ALL",
    #"anc_region": "ALL"
}

all_data = []

def generate_url(year, month, base_url, params):
    start_datetime = datetime.datetime(year, month, 1, 8, 0).strftime("%Y%m%dT%H:%M-0000")
    if month == 12:
        end_datetime = datetime.datetime(year + 1, 1, 1, 8, 0).strftime("%Y%m%dT%H:%M-0000")
    else:
        next_month = month + 1
        next_year = year
        end_datetime = datetime.datetime(next_year, next_month, 1, 8, 0).strftime("%Y%m%dT%H:%M-0000")

    params["startdatetime"] = start_datetime
    params["enddatetime"] = end_datetime

    url = base_url + "&".join(f"{key}={value}" for key, value in params.items())
    return url


def extract_csv_from_zip(zip_content):
    all_csv_content = []
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith(".csv"):
                with zip_ref.open(file_name) as csv_file:
                    csv_content = csv_file.read().decode("utf-8")
                    all_csv_content.append(csv_content)
    return '\n'.join(all_csv_content) if all_csv_content else None

def process_month_data(year, month, output_path, base_url, params, all_data):
    url = generate_url(year, month, base_url, params)
    print("URL:", url)

    response = requests.get(url)
    print(response.status_code)
    if response.status_code == 200:
        # Extract the CSV file from the ZIP archive
        csv_content = extract_csv_from_zip(response.content)
        if csv_content:
            csvreader = csv.reader(csv_content.splitlines(), delimiter=",")
            data = list(csvreader)
            df = pd.DataFrame(data)
            all_data.append(df)
            print(f"Data downloaded for {year}-{month:02d}")
        else:
            print(f"No data found in response for {year}-{month:02d}")
    else:
        print(f"Error downloading data for {year}-{month:02d}: {response.status_code}")

    time.sleep(15)

for year in range(start_year, end_year+1):
    print(f"Pulling year: {year}")
    for month in range(start_month, end_month+1):
        print(f"Pulling month: {month}")
        process_month_data(year, month, output_path, base_url, params, all_data)

# Combine all_data into a single DataFrame and export to CSV
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    
    combined_df.columns = combined_df.iloc[0]
    combined_df = combined_df[1:]
    
    combined_df.sort_values(by=["INTERVALSTARTTIME_GMT", "ANC_TYPE", "ANC_REGION"], inplace=True)
    
    combined_df.to_csv(output_path, index=False)
    print(f"Data successfully exported to: {output_path}")
else:
    print("No data downloaded.")
