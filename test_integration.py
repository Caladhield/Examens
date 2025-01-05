import pytest
import os
import pyodbc
import requests

API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
HEADERS = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": os.getenv("API_KEY"),
}

connection_string = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:cryptodatabase.database.windows.net,1433;"
    "Database=CryptoDB2;"
    f"Uid={os.getenv('SQL_SERVER_USER')};"
    f"Pwd={os.getenv('SQL_SERVER_PASSWORD')};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
    "Authentication=ActiveDirectoryPassword"
)

def test_api_connection():
    """Test that the API fetches data successfully."""
    response = requests.get(API_URL, headers=HEADERS, params={"limit": "1", "convert": "USD"})
    assert response.status_code == 200, "API request failed"
    data = response.json().get("data", [])
    assert len(data) > 0, "API returned no data"

def test_database_connection():
    """Test that the database connection works."""
    try:
        conn = pyodbc.connect(connection_string)
        assert conn is not None, "Failed to establish a database connection"
        conn.close()
    except Exception as e:
        assert False, f"Database connection failed: {e}"
