import requests
import pandas as pd
import os


def data_creation():
    urls = [
        'https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/SalaryData/FeatureServer/0' # noqa
    ]

    batch_size = 1000
    data_list = []
    output_directory = 'data'
    output_file = os.path.join(output_directory, 'salary_data.csv')

    if os.path.exists(output_file):
        print(f"Data already exists at {output_file}. No need to fetch.")
        return

    for url in urls:
        offset = 0

        while True:
            params = {
                'where': '1=1',
                'outFields': '*',
                'returnGeometry': 'false',
                'resultOffset': offset,
                'resultRecordCount': batch_size,
                'f': 'json'
            }

            response = requests.get(f"{url}/query", params=params)
            response.raise_for_status()

            query_result = response.json()
            features = query_result.get('features', [])

            for feature in features:
                data_list.append(feature['attributes'])

            if len(features) == 0:
                break

            offset += batch_size

    df = pd.DataFrame(data_list)

    os.makedirs(output_directory, exist_ok=True)
    df.to_csv(output_file, index=False)

    print(f"Data saved to {output_file}")