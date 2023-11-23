"""
An international firm that is looking to expand its business in diff. countries across the world has recruited you.
You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the
list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the
International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by
the organization to extract the information as it is updated.
Your boss wants you to demonstrate the success of this code by running a query on the database table to display only
the entries with more than a 100 billion USD economy.
Also, you should log in a file with the entire process of execution named etl_project_log.txt.
"""

import sqlite3
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime

URL = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
tb_name = 'Countries_by_GDP'
csv = 'Countries_by_GDP.csv'


def extract(url, table_attributes):
    """This function extracts data from the webpage"""
    page = requests.get(url, timeout=10).text
    data = BeautifulSoup(page, 'html.parser')
    dataset = pd.DataFrame(columns=table_attributes)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                dataset = pd.concat([dataset, df1], ignore_index=True)
    return dataset


def transform(dataset):
    """This function converts the gdp amount from millions to billions rounded
    to 2 decimal places."""
    gdp_list = dataset["GDP_USD_millions"].tolist()
    gdp_list = [float("".join(x.split(','))) for x in gdp_list]
    gdp_list = [np.round(x/1000, 2) for x in gdp_list]
    dataset["GDP_USD_millions"] = gdp_list
    dataset = dataset.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return dataset


def load_to_csv(dataset, csv_path):
    """This function stores the dataset in a csv file"""
    dataset.to_csv(csv_path)


def load_to_db(dataset, sql_connection, table_name):
    """This function stores the dataset in a table into a database"""
    dataset.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    """This function queries the database"""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message):
    """This function logs every proces in a log file"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(URL, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv)
log_progress('Data saved to CSV file')
sql_conn = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_conn, tb_name)
log_progress('Data loaded to Database as table. Running the query')
sql_statement = f"SELECT * from {tb_name} WHERE GDP_USD_billions >= 100"
run_query(sql_statement, sql_conn)
log_progress('Process Complete.')
sql_conn.close()
