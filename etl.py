import sqlalchemy as sa
import pyodbc
import pandas as pd
import requests
from sqlalchemy.orm import sessionmaker
import json
from pandas import DataFrame

###### Extract

token = ""
headers = {
        "Accept": "application/json",
        'Content-Type': 'application/json',
        "Accept-Encoding": "deflate, gzip",
        "X-CMC_PRO_API_KEY": token
}
r = requests.get('https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest', headers=headers)


###### Transform

data = r.json()["data"]

df = pd.DataFrame.from_dict(data)
#extract columns from 'quoute' dictionary
df2 = pd.json_normalize(df['quote'])
#merge extracted columns 
df = df.join(df2, lsuffix="_left", rsuffix="_right")


def validation(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No data downloaded")
        return False
        
    if pd.Series(df['id']).is_unique:
        pass
    else: 
        raise Exception("Coin id is not unique")

    if df['id'].isnull().values.any():
        raise Exception("Column id contain null values")
    else: 
        pass


def replace(df: pd.DataFrame) ->DataFrame:
    #replace NaN with None
    df = df.replace({pd.np.nan: None})
    #remove 'USD.' from column names
    df.columns = df.columns.str.replace('USD.', '')
    #drop not needed columns
    df = df.drop(columns=['slug', 'platform', 'self_reported_circulating_supply', 'self_reported_market_cap',
    'last_updated', 'quote'])
    #convert list to string
    df['tags'] = [','.join(map(str, l)) for l in df['tags']]
    return df

validation(df)
df = replace(df)

###### Load

server = "DESKTOP-QP9CDQT"
database = 'cmc'
driver = 'ODBC Driver 17 for SQL Server'
Database_Con = f'mssql+pyodbc://@{server}/{database}?driver={driver}'

engine = sa.create_engine(Database_Con)
con = engine.connect()

create_table = """
    CREATE TABLE IF NOT EXISTS coins (
        id INT PRIMARY KEY NOT NULL,
        name VARCHAR(200) NOT NULL,
        symbol VARCHAR(10),
        num_market_pairs INT,
        date_added DATE,
        tags VARCHAR(MAX),
        max_supply FLOAT,
        circulating_supply FLOAT,
        total_supply FLOAT,
        cmc_rank INT,
        tvl_ratio FLOAT,
        price FLOAT,
        volume_24h FLOAT,
        volume_change_24h FLOAT,
        percent_change_1h FLOAT,
        percent_change_24h FLOAT,
        percent_change_7d FLOAT,
        percent_change_30d FLOAT,
        percent_change_60d FLOAT,
        percent_change_90d FLOAT,
        market_cap FLOAT,
        market_cap_dominance FLOAT,
        fully_diluted_market_cap FLOAT,
        tvl FLOAT
    )
"""

con.execute(create_table)
print("Table created")

try:
    df.to_sql(name='dbo.dbo.coins', con=engine, if_exists='append')
except:
    print("Data already exists")

con.close()