import requests
import pandas as pd
from sqlalchemy import create_engine, exc

# CoinMarketCap API configuration
API_KEY = "fd56f227-193b-4a28-8a77-93f7542caeca"
API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
HEADERS = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": API_KEY,
}

# SQLAlchemy connection string (adjust as needed)
DB_ENGINE = create_engine("mssql+pyodbc://THOMAS-PC\\SQLEXPRESS/CryptoDB?driver=ODBC+Driver+17+for+SQL+Server")


# Function to fetch data from CoinMarketCap API
def fetch_crypto_data():
    params = {
        "start": "1",  # Starting rank
        "limit": "100",  # Number of cryptocurrencies to fetch
        "convert": "USD",  # Convert to USD
    }
    response = requests.get(API_URL, headers=HEADERS, params=params)
    if response.status_code == 200:
        return pd.DataFrame(response.json()["data"])
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return pd.DataFrame()


# Function to save data to SQL Server using SQLAlchemy
def save_to_db(dataframe):
    # Transform the DataFrame into a suitable format for the database
    transformed_data = dataframe.apply(
        lambda row: {
            "Symbol": row["symbol"],
            "Name": row["name"],
            "Price": row["quote"]["USD"]["price"],
            "MarketCap": row["quote"]["USD"]["market_cap"],
            "TotalSupply": row["total_supply"],
            "LastUpdated": row["last_updated"],
        },
        axis=1,
    )
    transformed_df = pd.DataFrame(list(transformed_data))

    # Ensure no duplicate data is inserted based on the 'id' field
    existing_data = pd.read_sql("SELECT Id FROM Cryptocurrency", DB_ENGINE)
    new_data = transformed_df[~transformed_df["Symbol"].isin(existing_data["Id"])]

    if not new_data.empty:
        # Insert new data into the database
        try:
            new_data.to_sql("Cryptocurrency", DB_ENGINE, if_exists="append", index=False)
            print(f"Inserted {len(new_data)} new rows into the database.")
        except exc.SQLAlchemyError as e:
            print(f"Error inserting data: {e}")
    else:
        print("No new data to insert.")


# Function to load data from SQL Server (Optional for verification)
def load_data_from_db():
    query = "SELECT * FROM Cryptocurrency"
    return pd.read_sql(query, DB_ENGINE)


# Main execution
if __name__ == "__main__":
    # Fetch data from CoinMarketCap API
    print("Fetching data from CoinMarketCap...")
    crypto_data = fetch_crypto_data()

    if not crypto_data.empty:
        print("Saving data to SQL Server...")
        save_to_db(crypto_data)

        # Optional: Load data from DB to verify insertion
        print("Loading data from the database...")
        db_data = load_data_from_db()
        print("Data loaded successfully:")
        print(db_data.head())
    else:
        print("No data fetched.")
