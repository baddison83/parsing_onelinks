This respository contains an MVP ETL built for the purpose of parsing query params in url, organizing the resulting data, and pushing the data into a Snowflake data warehouse

Steps:
- SQL query to pull onelink urls from Snowflake warehouse
- Unpack onelink urls 
- Extract query parameters from urls
- Organize the query params and values in a pandas dataframe
- Push the resulting data into Snowflake

