# ETL operations on banks market cap data

# import section
import requests
import sqlite3
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


def extract(url, table_attribs):
    # Scraping data and storing to a dataframe
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    rows.pop(0)
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0 :
            data_dict = {"Name": col[1].find_all('a')[-1].text,
                        "MC_USD_Billion": float(col[2].contents[0].strip())}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, rates_csv_path):
    # transform MC (market cap) to respective currencies and add to dataframe
    rates_df = pd.read_csv(rates_csv_path)
    rates = rates_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x*rates['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*rates['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_CAD_Billion'] = [np.round(x*rates['CAD'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_csv_path):
    df.to_csv(output_csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    # log message and timestamp in the log file
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ',' + message + '\n')

# url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
# table_attribs = ['Name', 'MC_USD_Billion']
# db_name = 'Banks_MC.db'
# table_name = 'banks'
# output_csv_path = './Largest_Banks.csv'

# # ----------------- ETL PROCESS ----------------
# log_progress("Preliminaries complete. Initiating ETL Process")

# extracted_data = extract(url, table_attribs)

# log_progress("Data extraction complete. Initiating Transformation process")

# transformed_data = transform(extracted_data, "./exchange_rates.csv")

# log_progress("Data transformation complete. Initiating Loading process")

# load_to_csv(transformed_data, output_csv_path)

# log_progress("Data saved to CSV file")

# sql_connection = sqlite3.connect(db_name)

# log_progress("SQL Connection initiated")

# load_to_db(transformed_data, sql_connection, table_name)

# log_progress("Data loaded to Database as a table, Executing queries")

# query_statement = f"SELECT * FROM {table_name}"
# run_query(query_statement, sql_connection)

# query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
# run_query(query_statement, sql_connection)

# query_statement = f"SELECT Name from {table_name} LIMIT 5"
# run_query(query_statement, sql_connection)

# sql_connection.close()

# log_progress("Server Connection closed")

