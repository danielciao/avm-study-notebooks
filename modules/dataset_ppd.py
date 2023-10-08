import modules.utils as utils
import pandas as pd


def load_data(year=2018, use_saved=True, london_only=False):
    saved_name = 'saved_all_ppd'

    if use_saved:
        saved_df = utils.load_data(
            saved_name,
            lambda f: pd.read_csv(
                f, index_col='PPD_ID', parse_dates=['PPD_TransferDate'], low_memory=False
            )
        )

        if saved_df is not None:
            return saved_df.query('PPD_County == "GREATER LONDON"') if london_only else saved_df

    with utils.Timer() as t:
        t.log(f'Downloading PPD for {year}')

        source = f'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{year}.csv'
        headers = ['ID', 'Price', 'TransferDate', 'Postcode', 'PropertyType', 'OldNew', 'Duration', 'PAON',
                   'SAON', 'Street', 'Locality', 'TownCity', 'District', 'County', 'Category', 'RecordStatus']

        df = pd.read_csv(source, header=None, names=headers,
                         true_values=['Y'], false_values=['N'], parse_dates=['TransferDate'], low_memory=False)

        # Add index
        df = df.add_prefix('PPD_')
        df.set_index('PPD_ID', inplace=True)

        if london_only:
            df = df.query('PPD_County == "GREATER LONDON"')

            print(f'{year} London data info:')
            print('Best selling postcode.\n', df['PPD_Postcode'].value_counts(
            ).reset_index(name='count').head(10))
            print('First and last date of transfer:\n',
                  df['PPD_TransferDate'].agg(['min', 'max']))

        t.log(f'Downloaded {len(df)} PPD records')

    return df
