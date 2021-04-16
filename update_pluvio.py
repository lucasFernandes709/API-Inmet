import pandas as pd
import numpy as np
import requests
from datetime import datetime as dt


def print_valid_dates(formats):
    print(formats)


def get_date(str_date: str):
    _valid_formats_base = ['%d-%m-%Y', '%Y-%m-%d']
    _valid_formats_variations = [(d,
                                  d.replace('-', '/'),
                                  d.replace('Y', 'y'),
                                  d.replace('-', '/').replace('Y', 'y'))
                                 for d in _valid_formats_base]
    _valid_formats_list = [i for t in _valid_formats_variations for i in t]

    for format in _valid_formats_list:
        try:
            return dt.strptime(str_date, format).date()
        except:
            pass

    print(f'Formato da data {str_date} inválido. Processo abortado!')
    print_valid_dates(_valid_formats_base)
    return False


# Base URL to access info
URL_TEMPLATE = 'https://apitempo.inmet.gov.br/estacao/{}/{}/{}'

# Parameter collection
# start_date = np.datetime64('2020-01-01')
# end_date = np.datetime64('2021-04-13')
start_date = get_date('2020-01-01')
end_date = get_date('2021-04-13')

stations = [('Marabá', 'A240'), ('Xinguara', 'A247')]
final_df = pd.DataFrame()

# Loop through stations list
for station_name, station_cod in stations:

    # Requisitions list (will be passed to the API)
    req = [(start_date, end_date, station_cod)]

    # Check if dates interval is greater than 6 months
    date_diff = (end_date - start_date).astype(int)
    if date_diff > 180:
        date_intervals = np.floor(date_diff / 180).astype(int)

        # Generate new requisitions list, limiting date span to 180 days
        req = []
        start_date_int = start_date
        for i in range(1, date_intervals + 1):
            end_date_interval = start_date + 180 * i
            req.append((str(start_date_int), str(end_date_interval), station_cod))
            start_date_int = end_date_interval + 1

        req.append((str(start_date_int), str(end_date), station_cod))

    # Get data from Inmet API for all requisitions
    for r in req:
        dados = requests.get(URL_TEMPLATE.format(*r)).json()

        # Convert to DataFrame format
        df = pd.DataFrame.from_records(dados)
        df = df.rename(columns={'CHUVA': 'Pluviometria', 'DT_MEDICAO': 'Data'})

        # Indentify data types and aggregate by day
        df_rain = pd.to_numeric(df['Pluviometria'])
        df_station = pd.concat([df['Data'], df_rain], axis=1)

        df_agg = df_station.groupby(by='Data')['Pluviometria'].sum().round(2)
        df_agg.index = pd.to_datetime(df_agg.index, format='%Y-%m-%d').strftime('%d-%m-%Y')
        df_agg = df_agg.reset_index()

        # Insert new columns
        df_agg['Estação'] = station_name

        # Concat to general DataFrame
        final_df = final_df.append(df_agg, ignore_index=True)

    # Save final DataFrame to .CSV file
    final_df.to_csv('final_df.csv', encoding='utf-8-sig', decimal=',', index=False)
