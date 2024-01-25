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
    rows = tables[2].find_all('tr')
    rows.pop(0)
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0 :
            if col[0].find('a') is not None and '—' not in col[2]:
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
    df['MC_INR_Billion'] = [np.round(x*rates['INR'],2) for x in df['MC_USD_Billion']]

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

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks_MC.db'
table_name = 'banks'
output_csv_path = './Countries_by_GDP.csv'

