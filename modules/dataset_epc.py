import os

import modules.utils as utils
import numpy as np
import pandas as pd


def load_data(boroughs_df):
    input_folder = './data/input/epc'
    saved_name = 'saved_all_epc_2023'
    file_count = 1
    row_count = 0

    saved_df = utils.load_data(
        saved_name,
        lambda f: pd.read_csv(
            f, index_col='EPC_LMK_KEY', parse_dates=['EPC_INSPECTION_DATE'], low_memory=False
        )
    )

    if saved_df is not None:
        return saved_df

    boroughs = boroughs_df.set_index('ONSUD_BOROUGH_CODE')[
        'ONSUD_BOROUGH'
    ].to_dict()

    columns = [
        'LMK_KEY', 'UPRN', 'INSPECTION_DATE',
        'ADDRESS', 'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'POSTCODE',
        'TOTAL_FLOOR_AREA', 'FLOOR_LEVEL', 'FLOOR_HEIGHT', 'NUMBER_HABITABLE_ROOMS',
        'PROPERTY_TYPE', 'BUILT_FORM', 'CONSTRUCTION_AGE_BAND', 'TENURE',
        'CURRENT_ENERGY_RATING', 'WINDOWS_ENERGY_EFF', 'WALLS_ENERGY_EFF', 'ROOF_ENERGY_EFF', 'MAINHEAT_ENERGY_EFF', 'LIGHTING_ENERGY_EFF',
        'ENERGY_CONSUMPTION_CURRENT', 'CO2_EMISSIONS_CURRENT']

    files = []
    with utils.Timer() as t:
        for root, dirs, _ in os.walk(input_folder):
            for dir in dirs:
                code = dir.split('-')[1]
                b = boroughs.get(code)

                if b:
                    t.log(f'{file_count}/{len(boroughs)} Reading EPC for {b}')

                    source = os.path.join(root, dir, 'certificates.csv')
                    current_df = pd.read_csv(source, header=0, usecols=columns, parse_dates=[
                        'INSPECTION_DATE'], index_col=False, low_memory=False)

                    files.append(current_df)
                    row_count += len(current_df)
                    file_count += 1

        t.log(f'Loaded {row_count} EPC records')

    df = pd.concat(files, axis=0)
    df = df.add_prefix('EPC_')
    df.set_index('EPC_LMK_KEY', inplace=True)

    utils.save_data(df, saved_name)

    return df


def enrich_epc_data(df, uprn_df):
    # Office for National Statistics (ONS) URPN Directory with ward and borough information
    uprn_df = uprn_df[['ONSUD_BOROUGH', 'ONSUD_WARD',
                       'ONSUD_OA', 'ONSUD_MSOA', 'ONSUD_LSOA', 'UPRN_LATITUDE', 'UPRN_LONGITUDE']]

    # left join EPC with UPRN data
    df = df.merge(uprn_df, how='left', left_on='EPC_UPRN', right_index=True)

    df_first_inspection = (df.reset_index()
                           .sort_values(by='EPC_INSPECTION_DATE')
                           .drop_duplicates('EPC_UPRN', keep='first')[['EPC_UPRN', 'EPC_INSPECTION_DATE']]
                           .rename(columns={'EPC_INSPECTION_DATE': 'EPC_FIRST_INSPECTION_DATE'})
                           .set_index('EPC_UPRN'))

    # Add first inspection date
    df = df.merge(df_first_inspection, how='left',
                  left_on='EPC_UPRN', right_index=True)

    return df
