import requests
import time
import pendulum
import pandas as pd

# Change this time for fetching data
REFRESH_INTERVAL = 10

# Directory in which reports are saved
REPORTS_DIR = 'reports'

# URL's to fetch data
URLS = [
        'positions',
        'pending',
        'mtm',
        ]

def save_csv_file(data, filename):
    """
    Convert the file to csv format and save with the given filename
    data
        HTTP response with JSON
    filename
        filename to same
    """
    json_data = data.json()
    if type(json_data) == list:
        df = pd.DataFrame(json_data)
        df.index = df.index+1
        df.to_csv(filename)
    elif type(json_data) == dict:
        s = pd.Series(json_data).to_csv(filename)

while True:
    try:
        for url in URLS:
            response = requests.get(f"http://127.0.0.1:8181/{url}")
            filename = f"{REPORTS_DIR}/{url}.csv"
            save_csv_file(response, filename)
            print(pendulum.now())
        # Write report already creates a csv file
        req = requests.get("http://127.0.0.1:8181/write_report")
        print(req.text)
        time.sleep(REFRESH_INTERVAL)
    except Exception as e:
        print(e, url)

