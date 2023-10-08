import glob
import os

import modules.utils as utils
import pandas as pd


def load_data(london_only=False):
    input_folder = './data/input/onsud_feb_2023'
    saved_name = 'saved_onsud_feb_2023'
    row_count = 0

    saved_df = utils.load_data(
        saved_name,
        lambda f: pd.read_csv(
            f, index_col='ONSUD_UPRN', low_memory=False
        )
    )

    if saved_df is not None:
        return saved_df[saved_df['ONSUD_REGION'] == 'E12000007'] if london_only else saved_df

    all_files = glob.glob(os.path.join(input_folder, 'Data', '*.csv'))
    print(f'Loading saved data from {input_folder}...')

    local_df = pd.read_csv(os.path.join(
        input_folder, 'Documents', 'LAD_names and codes UK as at 12_22.csv'), encoding='ISO-8859-1')
    ward_df = pd.read_csv(os.path.join(
        input_folder, 'Documents', 'WD_Ward names and codes UK as at 12_22.csv'), encoding='ISO-8859-1')

    rename_dict = {
        'RGN22CD': 'REGION',
        'PCDS': 'POSTCODE',
        'LAD22NM': 'BOROUGH',
        'LAD22CD': 'BOROUGH_CODE',
        'WD22NM': 'WARD',
        'WD22CD': 'WARD_CODE',
        'OA21CD': 'OA',
        'msoa21cd': 'MSOA',
        'lsoa21cd': 'LSOA'
    }

    columns = ['UPRN'] + list(rename_dict.values())

    files = []
    with utils.Timer() as t:
        for count, filename in enumerate(all_files, start=1):
            t.log(f'{count}/{len(all_files)} Reading postcode data from {filename}')

            df = pd.read_csv(filename, low_memory=False)

            if london_only:
                df = df[df['RGN22CD'] == 'E12000007']

            if not df.empty:
                df = df.merge(local_df, how='left', on='LAD22CD')
                df = df.merge(ward_df, how='left', on='WD22CD')
                df = df.rename(columns=rename_dict)
                df = df[columns]
                df = df.apply(
                    lambda x: x.str.strip() if x.dtype == "object" else x)

                files.append(df)
                row_count += len(df)

        t.log(f'Loaded {row_count} postcode records')

    df = pd.concat(files, ignore_index=True)
    df = df.add_prefix('ONSUD_')
    df.set_index('ONSUD_UPRN', inplace=True)

    utils.save_data(df, saved_name)

    return df
