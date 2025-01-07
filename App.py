import os
import requests
import pandas as pd
import pyodbc
from datetime import datetime
import pytz

# CoinMarketCap API configuration
API_KEY = os.getenv("API_KEY")
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD")

API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
HEADERS = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": API_KEY,
}

connection_string = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:cryptodatabase.database.windows.net,1433;"
    "Database=CryptoDB2;"
    f"Uid={SQL_SERVER_USER};"
    f"Pwd={SQL_SERVER_PASSWORD};"
    "Encrypt=yes;" 
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
    "Authentication=ActiveDirectoryPassword"
)

try:
    conn = pyodbc.connect(connection_string)
    print("Connection successful!")
    
    cursor = conn.cursor()

except Exception as e:
    print(f"Error: {e}")

# Function to get data from CoinMarketCap API
def fetch_crypto_data():
    params = {
        "start": "1",  # Starting rank
        "limit": "300",  # Number of cryptocurrencies to retrieve
        "convert": "USD",
    }
    response = requests.get(API_URL, headers=HEADERS, params=params)
    if response.status_code == 200:
        return pd.DataFrame(response.json()["data"])
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return pd.DataFrame()


# Function to convert UTC to UTC+1 to avoid inaccurate time through Github Actions 
def convert_to_utc_plus_1(utc_timestamp):
    utc_time = utc_timestamp.replace(tzinfo=pytz.utc)
    return utc_time.astimezone(pytz.timezone('Europe/Brussels'))

# Transform raw data into metadata and market data
def transform_data(raw_data):
    # Metadata transformation
    metadata = raw_data[["id", "name", "symbol", "slug", "date_added"]].copy()
    metadata.rename(
        columns={
            "id": "Id",
            "name": "Name",
            "symbol": "Symbol",
            "slug": "Slug",
            "date_added": "DateAdded",
        },
        inplace=True,
    )
    
    # Market data transformation
    market_data = raw_data.apply(
        lambda row: {
            "CryptocurrencyId": row["id"],
            "Price": row["quote"]["USD"].get("price", None),
            "MarketCap": row["quote"]["USD"].get("market_cap", None),
            "CirculatingSupply": row.get("circulating_supply", None),
            "TotalSupply": row.get("total_supply", None),
            "MarketCapDominance": row["quote"]["USD"].get("market_cap_dominance", None),
            "PercentChange1H": row["quote"]["USD"].get("percent_change_1h", None),
            "PercentChange24H": row["quote"]["USD"].get("percent_change_24h", None),
            "VolumeChange24H": row["quote"]["USD"].get("volume_change_24h", None),
            "LastUpdated": row["quote"]["USD"].get("last_updated", None),
            "RecordTimestamp": convert_to_utc_plus_1(datetime.utcnow()),
        },
        axis=1,
    )
    market_data_df = pd.DataFrame(list(market_data))
    

    numeric_columns = [
        "Price", "MarketCap", "CirculatingSupply", "TotalSupply",
        "MarketCapDominance", "PercentChange1H", "PercentChange24H", "VolumeChange24H"
    ]
    for column in numeric_columns:
        market_data_df[column] = pd.to_numeric(market_data_df[column], errors="coerce")
    
    return metadata, market_data_df

# Insert metadata into the database
def save_metadata_to_db(metadata_df):
    # Check existing symbols to avoid duplicates
    cursor = conn.cursor()
    cursor.execute("SELECT Symbol FROM CryptocurrencyMetadata")
    existing_symbols = [row[0] for row in cursor.fetchall()]
    
    new_metadata = metadata_df[~metadata_df["Symbol"].isin(existing_symbols)]
    if not new_metadata.empty:
        for index, row in new_metadata.iterrows():
            cursor.execute("""
                INSERT INTO CryptocurrencyMetadata (Id, Name, Symbol, Slug, DateAdded)
                VALUES (?, ?, ?, ?, ?)
            """, row["Id"], row["Name"], row["Symbol"], row["Slug"], row["DateAdded"])
        conn.commit()
        print(f"Inserted {len(new_metadata)} new metadata rows.")

# Insert market data into the database
def save_market_data_to_db(market_data_df):
    try:
        cursor = conn.cursor()
        for index, row in market_data_df.iterrows():
            cursor.execute("""
                INSERT INTO CryptocurrencyMarketData (CryptocurrencyId, Price, MarketCap, CirculatingSupply, TotalSupply, 
                MarketCapDominance, PercentChange1H, PercentChange24H, VolumeChange24H, LastUpdated, RecordTimestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row["CryptocurrencyId"], row["Price"], row["MarketCap"], row["CirculatingSupply"], row["TotalSupply"],
                row["MarketCapDominance"], row["PercentChange1H"], row["PercentChange24H"], row["VolumeChange24H"], 
                row["LastUpdated"], row["RecordTimestamp"])
        conn.commit()
        print(f"Inserted {len(market_data_df)} market data rows.")
    except Exception as e:
        print(f"Error saving market data: {e}")


if __name__ == "__main__":
    print("Fetching data from CoinMarketCap...")
    raw_data = fetch_crypto_data()

    if not raw_data.empty:
        print("Transforming data...")
        metadata, market_data = transform_data(raw_data)

        print("Saving metadata to SQL Server...")
        save_metadata_to_db(metadata)

        print("Saving market data to SQL Server...")
        save_market_data_to_db(market_data)

        print("Data successfully saved.")
    else:
        print("No data fetched.")

    # Close the database connection
    conn.close()
