import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# CoinMarketCap API configuration
API_KEY = "fd56f227-193b-4a28-8a77-93f7542caeca"
API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
HEADERS = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": API_KEY,
}

# SQLAlchemy connection string
DB_ENGINE = create_engine("mssql+pyodbc://THOMAS-PC\\SQLEXPRESS/CryptoDB?driver=ODBC+Driver+17+for+SQL+Server")

# Fetch data from CoinMarketCap API
def fetch_crypto_data():
    params = {
        "start": "1",  # Starting rank
        "limit": "300",  # Number of cryptocurrencies to fetch
        "convert": "USD",
    }
    response = requests.get(API_URL, headers=HEADERS, params=params)
    if response.status_code == 200:
        return pd.DataFrame(response.json()["data"])
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return pd.DataFrame()

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
            "RecordTimestamp": datetime.now(),
        },
        axis=1,
    )
    market_data_df = pd.DataFrame(list(market_data))
    
    # Ensure numeric values are within bounds
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
    existing_symbols = pd.read_sql("SELECT Symbol FROM CryptocurrencyMetadata", DB_ENGINE)
    new_metadata = metadata_df[~metadata_df["Symbol"].isin(existing_symbols["Symbol"])]
    if not new_metadata.empty:
        new_metadata.to_sql("CryptocurrencyMetadata", DB_ENGINE, if_exists="append", index=False)
        print(f"Inserted {len(new_metadata)} new metadata rows.")

# Insert market data into the database
def save_market_data_to_db(market_data_df):
    try:
        market_data_df.to_sql("CryptocurrencyMarketData", DB_ENGINE, if_exists="append", index=False)
        print(f"Inserted {len(market_data_df)} market data rows.")
    except Exception as e:
        print(f"Error saving market data: {e}")

# Main execution
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
