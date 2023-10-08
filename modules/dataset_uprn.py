import os

import pandas as pd

# Define the columns to be returned
RETURN_COLUMNS = ['UPRN_LATITUDE', 'UPRN_LONGITUDE']


def load_data(london_only=False):
    saved_name = 'osopenuprn_202301'
    parquet_file = os.path.join('./data/saved', f'{saved_name}.parquet')

    if os.path.exists(parquet_file):
        print(f'Loading saved data from {parquet_file}...')
        df = pd.read_parquet(parquet_file)[RETURN_COLUMNS]
    else:
        # If the parquet file doesn't exist, load from the csv file
        cvs_file = os.path.join('./data/input', f'{saved_name}.csv')
        print(f'Loading data from {cvs_file}...')
        df = pd.read_csv(cvs_file, index_col='UPRN', low_memory=False)
        df = df.add_prefix('UPRN_')
        df.to_parquet(parquet_file)
        df = df[RETURN_COLUMNS]

    if london_only:
        df = df[(df['UPRN_LATITUDE'] > 51.2) & (df['UPRN_LATITUDE'] < 51.7) & (
            df['UPRN_LONGITUDE'] > -0.6) & (df['UPRN_LONGITUDE'] < 0.3)]

    return df
