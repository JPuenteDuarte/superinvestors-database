import pandas as pd
import requests
import os
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

def read_db_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

file_path = 'BD_connection.txt'
credentials = read_db_credentials(file_path)

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

def fetch_activity(fund,buy_or_sell, page):
    
    url = 'https://www.dataroma.com/m/m_activity.php?m=%s&typ=%s&L=%s&o=a' % (fund, buy_or_sell[0], page)

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'es-ES,es;q=0.9',
        'Cache-Control': 'max-age=0',
        'Cookie': '_ga=GA1.1.130284920.1719392925; FCCDCF=%5Bnull%2Cnull%2Cnull%2C%5B%22CQA0YQAQA0YQAEsACBENA6EgAAAAAEPgAAQAAAAOhQD2F2K2kKFkPCmQWYAQBCijYAAhQAAAAkCBIAAgAUgQAgFIIAgAIFAAAAAAAAAQEgCQAAQABAAAIACgAAAAAAIAAAAAAAQQAABAAIAAAAAAAAEAAAAAAAQAAAAIAABEhCAAQQAEAAAAAAAQAAAAAAAAAAABAAA%22%2C%222~~dv.70.89.93.108.122.149.196.236.259.311.313.323.358.415.449.486.494.495.540.574.609.827.864.981.1029.1048.1051.1095.1097.1126.1205.1211.1276.1301.1365.1415.1423.1449.1514.1570.1577.1598.1651.1716.1735.1753.1765.1870.1878.1889.1958.2072.2253.2299.2357.2373.2415.2506.2526.2568.2571.2575.2624.2677%22%2C%22F71CA2CC-75DB-4B63-A434-5826A5985A62%22%5D%5D; __eoi=ID=ac059e84a5250159:T=1719392940:RT=1722010550:S=AA-AfjZK0tKoYzgVT87zQpVwFEUd; _ga_53FSPN06Y3=GS1.1.1722009106.20.1.1722010551.58.0.0',
        'Referer': 'https://www.dataroma.com/m/m_activity.php?m=%s&typ=%s&L=%s&o=a' % (fund, buy_or_sell[0], page),
        'Sec-Ch-Ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.text
    else:
        print(f'Error: {response.status_code}')
        return None

def extract_strings(s, substring1, substring2):
    substrings = []
    start = 0
    while True:
        start_index = s.find(substring1, start)
        if start_index == -1:
            break   
        start_index += len(substring1)
        end_index = s.find(substring2, start_index)
        if end_index == -1:
            break
        substrings.append(s[start_index:end_index])
        start = end_index + len(substring2)
    return substrings

def extract_transactions(s, transaction_type, fund):
    substring1_symbol = """/m/hist/hist.php?f=%s&s=""" % fund
    substring2_symbol = """" title="Holding/activity history"""
    substring1_stock = """<td class="%s">""" % transaction_type
    substring2_stock = """</td>"""

    symbols = extract_strings(s, substring1_symbol, substring2_symbol)
    stocks = extract_strings(s, substring1_stock, substring2_stock)
    
    Activity = [stocks[i] for i in range(0, len(stocks)) if i % 2 == 0]
    Share_change = [stocks[i] for i in range(0, len(stocks)) if i % 2 != 0]

    lista = {
        'symbol': symbols,
        'share_change': Share_change
    }
    df_activity = pd.DataFrame(lista)
    df_activity['transaction_type'] = transaction_type
    if len(df_activity)>0:    
        df_activity['share_change'] = df_activity['share_change'].str.replace(',', '').astype(float)
    return df_activity

def dividir_por_delimitador(s):
    return s.split("""colspan="5"><b>""")

def fund_transactions(action_taken, fund):
    all_transactions_df = []
    for page in range(5):
        try:
            s_activity = fetch_activity(fund, action_taken, page)
            html_chunks = dividir_por_delimitador(s_activity)
            for chunk in html_chunks[1:]:
                temp_df = extract_transactions(chunk, action_taken, fund)
                temp_df['quarter'] = chunk[1]
                temp_df['year'] = chunk[15:19]
                temp_df['fund_code'] = fund
                all_transactions_df.append(temp_df)
        except:
            continue
    final_df = pd.concat(all_transactions_df)
    return(final_df)

def read_db_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

file_path = 'BD_connection.txt'

if not os.path.exists(file_path):
    raise FileNotFoundError(f"The file cannot be found in the given location: {file_path}")

credentials = read_db_credentials(file_path)

connection_string = f"postgresql+psycopg2://{credentials['username']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"
engine = create_engine(connection_string)

query = "SELECT * FROM superinvestors"

df = pd.read_sql(query, engine)

funds = df.iloc[:, 1].tolist()

all_funds_transactions = []
for fund in funds:
    fund_buy = fund_transactions('buy', fund)
    fund_sell = fund_transactions('sell', fund)
    all_funds_transactions.append(fund_buy)
    all_funds_transactions.append(fund_sell)
funds_activity = pd.concat(all_funds_transactions)

funds_activity.to_sql('activity', engine, if_exists='replace', index=False)

print("DataFrame exported correctly to the table 'activity'.")