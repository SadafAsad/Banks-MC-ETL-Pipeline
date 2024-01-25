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

