import os
import time

import joblib as jl
import pandas as pd


def format_ratio(dividend, divisor):
    return '%.4f' % (dividend * 100 / divisor) + '%'


def print_missing_values(df):
    missing_values = df.isnull().sum()
    missing_values = missing_values[missing_values > 0].sort_values(
        ascending=False)
    missing_percent = ((missing_values / df.shape[0]) * 100).round(2)

    if missing_values.empty:
        print("No missing values detected.")
    else:
        missing_df = pd.DataFrame(
            {'Missing Count': missing_values, 'Missing %': missing_percent})
        print(missing_df)


def load_data(saved_name, load_csv=None):
    parquet_file = os.path.join('./data/saved', f'{saved_name}.parquet')
    if os.path.exists(parquet_file):
        print(f'Loading saved data from {parquet_file}...')
        return pd.read_parquet(parquet_file)

    csv_file = os.path.join('./data/input', f'{saved_name}.csv')
    if os.path.exists(csv_file):
        if load_csv is None:
            raise Exception('Please provide a load_csv function')
        print(f'Loading data from {csv_file}...')
        df = load_csv(csv_file)
        df.to_parquet(parquet_file)
        return df


def load_saved_data(saved_name):
    df = load_data(saved_name)

    if df is None:
        raise Exception(f'No saved data found for {saved_name}')

    return df


def save_data(df, saved_name, as_csv=False):
    if as_csv:
        csv_file = os.path.join('./data/saved', f'{saved_name}.csv')
        print(f'Saving data to {csv_file}...')
        df.to_csv(csv_file)
    else:
        parquet_file = os.path.join('./data/saved', f'{saved_name}.parquet')
        print(f'Saving data to {parquet_file}...')
        df.to_parquet(parquet_file)


def save_model(model, saved_name):
    model_file = os.path.join('./data/saved', f'{saved_name}.joblib')
    print(f'Saving model to {model_file}...')
    jl.dump(model, model_file)


class Timer:
    def __init__(self):
        self.start_time = None

    def start(self):
        self.start_time = time.perf_counter()

    def log(self, message):
        elapsed_time = time.perf_counter() - (self.start_time or 0)
        print(f'=== {message} ({elapsed_time:0.4f}s) ===')

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        pass
