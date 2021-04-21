import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta


def print_valid_dates(formats):
    print(formats)


def get_date(str_date):
    _valid_formats_base = ['%d-%m-%Y', '%Y-%m-%d']
    _valid_formats_variations = [(d,
                                  d.replace('-', '/'),
                                  d.replace('Y', 'y'),
                                  d.replace('-', '/').replace('Y', 'y'))
                                 for d in _valid_formats_base]
    _valid_formats_list = [i for t in _valid_formats_variations for i in t]

    for format in _valid_formats_list:
        try:
            return datetime.strptime(str_date, format).date()
        except:
            pass

    print(f'Formato da data {str_date} inválido. Processo abortado!')
    print_valid_dates(_valid_formats_base)
    return False


def load_stations(file_path):
    with open(file_path, 'r') as f:
        station_list = f.readlines()

    return list(map(lambda x: x.rstrip('\n'), station_list))


def split_date_range(start_date, end_date, date_intervals):
    req = []
    start_date_int = start_date
    for i in range(1, date_intervals + 1):
        end_date_interval = start_date + timedelta(days=180) * i
        req.append((str(start_date_int), str(end_date_interval)))
        start_date_int = end_date_interval + timedelta(days=1)

    req.append((str(start_date_int), str(end_date)))

    return req


# Constants
URL_TEMPLATE = 'https://apitempo.inmet.gov.br/estacao/{}/{}/{}'     # Base URL to access info
DATE_LIMIT   = 180                                                  # Date range limit to 180 days

# Parameter collection
start_date = get_date('2020-01-01')
end_date = get_date('2021-04-13')
stations = load_stations('./stations.txt')

# Empty DataFrame initialization
final_df = pd.DataFrame()

# Check if dates interval is greater than 6 months (180 days)
date_diff = (end_date - start_date).days
if date_diff > DATE_LIMIT:
    # If true, the date range must be splitted in intervals of 180 days max
    date_intervals = np.floor(date_diff / DATE_LIMIT).astype(int)
else:
    # Default value (will not split the date range)
    date_intervals = 0

# Generate date requisition list
req = split_date_range(start_date, end_date, date_intervals)

# Loop through stations list
for station_cod in stations:
    
    # Get data from Inmet API for each requisition (adjusted)
    for r in req:
        r_adj = r + (station_cod,)
        dados = requests.get(URL_TEMPLATE.format(*r_adj)).json()

        # TODO: Add functionality to choose aggregation by day/hour

        # Convert to DataFrame format
        df = pd.DataFrame.from_records(dados)
        df = df.rename(columns={'CHUVA': 'Pluviometria', 'DT_MEDICAO': 'Data'})

        # Indentify data types and aggregate by day
        df_rain = pd.to_numeric(df['Pluviometria'])
        df_station = pd.concat([df['Data'], df_rain], axis=1)

        df_agg = df_station.groupby(by='Data')['Pluviometria'].sum().round(2)
        df_agg.index = pd.to_datetime(df_agg.index, format='%Y-%m-%d').strftime('%d-%m-%Y')
        df_agg = df_agg.reset_index()

        # Concat to general DataFrame
        final_df = final_df.append(df_agg, ignore_index=True)

    # Save final DataFrame to .CSV file
    final_df.to_csv('final_df.csv', encoding='utf-8-sig', decimal=',', index=False)
