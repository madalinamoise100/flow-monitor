import os
import pandas as pd
import json
import numpy as np
import datetime as dt
from datetime import datetime

valid_currencies = ['GBP', 'EUR', 'JPY']
valid_bond_types = ['Nominal', 'Floating', 'InfLinked']
desired_columns = ['time', 'tradeDate', 'nominal', 'dv01', 'recTrader',
                   'recEndState', 'security', 'platform', 'buySell',
                   'cName', 'isVoice', 'won', 'tradedAway', 'tiedAway',
                   'rejected', 'sign', 'countryCode', 'bondType', 'currency',
                   'securityClass', 'desk', 'sector', 'maturityDate',
                   'classification', 'Salesperson', 'RMHF']

TEST_DATASET = os.path.join(
    os.path.split(os.path.split(os.path.dirname(__file__))[0])[0],
    'data',
    'dummy_data.csv'
)


def _clean_data(df=None):
    # Remove unwanted currencies
    df = df[df.currency.isin(valid_currencies)]

    # Remove unwanted bond types
    df = df[df.classification.isin(valid_bond_types)]

    # Remove all rows with [NULL] values
    rows_with_null = df.apply(
       lambda row :
          any([ e == '[NULL]' for e in row ])
          , axis=1)
    df = df[~rows_with_null]

    df['dv01'] = df['dv01'].astype(float)

    # Turn trade and maturiy dates into datetime objects
    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
    # change format of maturity date - should not be needed for official data?
    df['maturityDate'] = pd.to_datetime(df['maturityDate'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

    # Return clean data
    return df


def _check_format(df=None):
    # Check that the number of columns is correct
    if len(desired_columns) < len(df.columns):
        raise Exception("The file you have provided has too many columns")
    elif len(desired_columns) > len(df.columns):
        raise Exception("The file you have provided does not have enough columns")
    # Check that the columns have the desired values
    for item in desired_columns:
        if item not in df.columns:
            raise Exception("Could not find column {}".format(item))
    # If checks passed, return True
    return True


def _permission_data(df=None, username=None):
    with open('permissions.json') as permissions_file:
        permissions_data = json.load(permissions_file)
        if username in permissions_data:
            permissioned_teams = []
            for team in permissions_data[username]:
                permissioned_teams.append(team)
        else:
            raise Exception('Username {} not found'.format(username))

    with open('teams.json') as teams_file:
        teams_data = json.load(teams_file)
        permissioned_trades = []
        for team in permissioned_teams:
            if team in teams_data:
                for salesperson in teams_data[team]:
                    permissioned_trades.append(salesperson)
            else:
                raise Exception('Team {} not found'.format(team))

    df = df[df.Salesperson.isin(permissioned_trades)]
    return df


def _filter_rm_hf(df=None, filter=None):
    df = df[df['RMHF'] == filter]
    return df


def _filter_by_date(df=None, from_date=None):
    from_date_date = dt.datetime.strptime(from_date, "%Y-%m-%d").date()

    print("FROM: ")
    df = df[df['tradeDate'] >= from_date_date]

    return df


def _create_table(df=None):
    valid_columns = ['countryCode', 'tradeDate', 'maturityDate', 'dv01']
    print(df.columns.values)
    for column in list(df.columns.values):
        if column not in valid_columns:
            df = df.drop(column, axis=1)

    df['maturityDate'] = [dt.datetime.strptime(md, '%Y-%m-%d').date() for md in df['maturityDate']]
    if df['tradeDate'].dtype == 'datetime64[ns]':
        df['tradeDate'] = df['tradeDate'].dt.date
        df['bond_tenor'] = df['maturityDate'] - df['tradeDate']
        df['bond_tenor'] = df['bond_tenor']/np.timedelta64(1, 'Y')

    df = df.groupby(['bond_tenor', 'countryCode']).sum().unstack()['dv01']
    print(df)

    return df


def _group_tenors(df):
    groupings = (
        [ 0, .25, .5, .75,] +
        list(range(1, 10)) +
        list(range(10, 20, 5)) +
        [20, 30]
    )

    # create an index from the groupings
    index = list(zip(*(groupings[:-1], groupings[1:])))

    cols = df.columns
    _df = df.reset_index()
    print(_df.columns)

    # this is teh new dataframe with the groupings
    df2 = pd.DataFrame(columns=cols)

    for (i, f,) in index:
        res = _df.loc[(_df['bond_tenor'] >= i) & (_df['bond_tenor'] < f), cols]
        # you didn't have the columns as type float!
        df2.loc[f'{i}-{f}', :] = res.astype(float).sum()

    # add the 30year data
    df2.loc['30+', :] = _df.loc[(_df['bond_tenor'] >= 30), cols].astype(float).sum()
    json_data = df2.reset_index().to_dict(orient="records")
    # data = []
    # data.append(json_data)
    return json_data

def get_data(username=None, filter=None, from_date=None):
    df = pd.read_csv(TEST_DATASET)

    try:
        valid_format = _check_format(df)
        if valid_format == True:
            df = _clean_data(df)
            df = _permission_data(df, username)
            df = _filter_rm_hf(df, filter)
            df = _filter_by_date(df, from_date)
            df = _create_table(df)
            data = _group_tenors(df)
            return data
        else:
            raise Exception("Unexpected error when checking data format")
    except Exception as e:
        raise Exception("Data has invalid format: {}".format(e))
