README: Cryptocurrency Data Pipeline Project

**Project Overview**

This project is designed to collect, transform, and visualize cryptocurrency data using modern data engineering tools and technologies. 
The pipeline fetches data from the CoinMarketCap API, processes it, and stores it in an Azure SQL Database. Power BI is used to create interactive dashboards for visualizing trends and insights.

**Technologies Used**

Programming Language:

  Python and SQL

Libraries & Frameworks:

  requests: For fetching data from the CoinMarketCap API
  pyodbc: To Connect to Azure SQL database
  pandas: For transforming and structuring the data
  DateTime/Pytz: To ensure timestamp consistency
  os: To manage environment variables securely
  
Database & Tools:

  Azure SQL Database: To store transformed cryptocurrency data
  Azure Data Studio: To manage and query the database

Visualization Tool:

  Power BI: For creating dashboards

Automation & CI/CD:

  GitHub Actions: For automating testing and deployment

**Project Components**

Data Collection:

  The pipeline fetches cryptocurrency data, including price, market capitalization, supply, and percentage changes, from the CoinMarketCap API.

Data Transformation:

  Raw data is split into two tables:
  
  CryptocurrencyMetadata: Contains static details like the cryptocurrency’s name, symbol, and date added to CoinMarketcap.
  CryptocurrencyMarketData: Contains dynamic metrics like price, market cap, and percentage changes.
  Data types are cleaned and verified for numeric fields.

Database Integration:

  The transformed data is stored in an Azure SQL Database, providing scalability and security.

Data Visualization:

  Power BI dashboards display key metrics like the latest price, market cap, and rank of selected cryptocurrencies.
  Interactive slicers and filters allow users to focus on specific cryptocurrencies or time periods.

Automation:

  GitHub Actions runs automated tests to verify the pipeline's integrity during deployment.
  Environment variables for database credentials and API keys are securely managed using GitHub Secrets.

**Setup Instructions**

  To have this application running on your local machine you must do the following:

  Clone the Repository

  Install Dependencies:
  Ensure you have Python installed. Then, run:

  pip install -r requirements.txt

Set Up Environment Variables:

Create a .env file and add the following variables:

  API_KEY=<your_coinmarketcap_api_key>
  SQL_SERVER_USER=<your_database_user>
  SQL_SERVER_PASSWORD=<your_database_password>

Or for github actions integration add your credentials to Github Secrets.

Run the Script

Power BI Configuration:

Connect Power BI to your Azure SQL Database.
Use the pre-configured relationships between the tables to create visuals.

**GitHub Workflow**

The project includes a GitHub Actions workflow that:

Runs automated tests.
Validates the database connection.
Deploys the code changes to production.

The workflow triggers automatically every 24 hours but can also be triggered manually.


**Future Improvements**

Integrate a front-end interface for easier access to the data.
Add more API endpoints for additional cryptocurrency metrics.
Expand the Dashboard with more visuals to compare certain cryptocurrency performances.


