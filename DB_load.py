import psycopg2
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import csv

def read_db_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

def connect_to_db():
    file_path = 'BD_connection.txt'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo no se encontró en la ruta especificada: {file_path}")
    
    credentials = read_db_credentials(file_path)
    
    conn = psycopg2.connect(
        dbname=credentials["database"],
        user=credentials["username"],
        password=credentials["password"],
        host=credentials["host"],
        port=credentials["port"]
    )
    return conn

def create_tables():
    conn = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS superinvestors (
                id VARCHAR(30),
                fund VARCHAR(255)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                symbol VARCHAR(10),
                name VARCHAR(255),
                industry VARCHAR(255),
                sub_industry VARCHAR(255)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS activity (
                id SERIAL,
                symbol VARCHAR(10),
                share_change INT,
                transaction_type VARCHAR(10),
                quarter INT,
                year INT,
                fund_code VARCHAR(10)
            );
        """)

        conn.commit()
        print("Tablas creadas exitosamente")

    except psycopg2.Error as e:
        print("Error al conectar a PostgreSQL:", e)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_tables()

def read_db_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

def fetch_table(url, headers):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            rows = soup.find_all('tr')
            if rows:
                data = []
                for row in rows:
                    row_data = []
                    cells = row.find_all('td')
                    if cells:
                        first_cell = cells[0]
                        link = first_cell.find('a')
                        if link and 'href' in link.attrs:
                            href = link['href']
                            arg = href.split('=')[-1]
                            row_data.append(link.get_text(strip=True))
                            row_data.append(arg)
                        else:
                            row_data.append(first_cell.get_text(strip=True))
                            row_data.append(None)  
                        
                        row_data.append(cells[1].get_text(strip=True))  
                        row_data.append(cells[2].get_text(strip=True))  
                        data.append(row_data)
                
                columns = ['Portfolio Manager - Firm', 'Code', 'Portfolio value', 'No. of stocks']
                superinv_df = pd.DataFrame(data, columns=columns)
                return superinv_df
            else:
                print("No rows found in the table.")
        else:
            print(f"Failed to fetch webpage: {url}, Status Code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

file_path = 'BD_connection.txt'
credentials = read_db_credentials(file_path)

connection_string = f"postgresql+psycopg2://{credentials['username']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"
engine = create_engine(connection_string)

url = "https://www.dataroma.com/m/managers.php"
headers = {"User-Agent": "your-user-agent"}
superinv_df = fetch_table(url, headers)

superinv_df.columns = superinv_df.iloc[0]
superinv_df = superinv_df[1:]
superinv_df = superinv_df.drop(columns=["Portfolio value", "No. of stocks"])
superinv_df = superinv_df.rename(columns={'Portfolio Manager - Firm': 'fund'})
superinv_df.rename(columns={None: 'id'}, inplace=True)

table_name = 'superinvestors'

with engine.connect() as connection:
    connection.execute(text(f'DROP TABLE IF EXISTS {table_name} CASCADE'))
    superinv_df.to_sql(table_name, engine, if_exists='append', index=False)

print("DataFrame exported correctly to the table 'superinvestors'.")

def read_db_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

file_path = 'BD_connection.txt'
credentials = read_db_credentials(file_path)

connection_string = f"postgresql+psycopg2://{credentials['username']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"
engine = create_engine(connection_string)

wikiurl = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
tables = pd.read_html(wikiurl)
table_companies = tables[0]
columns_drop = ['Headquarters Location','Date added','CIK','Founded']
table_companies = table_companies.drop(columns=columns_drop)
table_companies = table_companies.rename(columns={
    'Symbol': 'symbol',
    'Security': 'name',
    'GICS Sector': 'industry',
    'GICS Sub-Industry': 'sub_industry'
})

wikiurl_2 = 'https://en.wikipedia.org/wiki/S%26P/TSX_Composite_Index'
tables_2 = pd.read_html(wikiurl_2)
table_companies_2 = tables_2[3]
table_companies_2 = table_companies_2.rename(columns={
    'Ticker': 'symbol',
    'Company': 'name',
    'Sector [10]': 'industry',
    'Industry [10]': 'sub_industry'
})

wikiurl_3 = 'https://en.wikipedia.org/wiki/Russell_1000_Index'
tables_3 = pd.read_html(wikiurl_3)
table_companies_3 = tables_3[2]
table_companies_3 = table_companies_3[['Symbol','Company','GICS Sector','GICS Sub-Industry']]
table_companies_3 = table_companies_3.rename(columns={
    'Symbol': 'symbol',
    'Company': 'name',
    'GICS Sector': 'industry',
    'GICS Sub-Industry': 'sub_industry'
})

wikiurl_4 = 'https://en.wikipedia.org/wiki/Nasdaq-100'
tables_4 = pd.read_html(wikiurl_4)
table_companies_4 = tables_4[4]
table_companies_4 = table_companies_4[['Ticker','Company','GICS Sector','GICS Sub-Industry']]
table_companies_4 = table_companies_4.rename(columns={
    'Ticker': 'symbol',
    'Company': 'name',
    'GICS Sector': 'industry',
    'GICS Sub-Industry': 'sub_industry'
})

companies = pd.concat([table_companies, table_companies_2, table_companies_3, table_companies_4], axis=0)
companies.drop_duplicates(subset=['symbol'], keep='first', inplace=True)
companies.dropna(subset=['symbol'], inplace=True)

industry_duplicates = {
    'Healthcare': 'Health Care',
    'Consumer Cyclical': 'Consumer Discretionary',
    'Financials': 'Financial Services',
    'Consumer Staples':'Consumer Defensive',
    'Materials':'Basic Materials'
}

companies['industry'] = companies['industry'].replace(industry_duplicates)
companies['industry'].unique()

table_name = 'companies'

with engine.connect() as connection:
    companies.to_sql(table_name, engine, if_exists='append', index=False)

print("DataFrame exported correctly to the table 'companies'.")
