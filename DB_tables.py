import psycopg2
import os

def read_db_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

def connect_to_db():
    file_path = 'DB_connection.txt'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file was not found in the given path: {file_path}")
    
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
            CREATE TABLE IF NOT EXISTS companies (
                symbol VARCHAR(10),
                name VARCHAR(255),
                industry VARCHAR(255),
                sub_industry VARCHAR(255)
            );
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS superinvestors (
                id VARCHAR(30),
                fund VARCHAR(255) UNIQUE
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
        cursor.close()

    except psycopg2.Error as e:
        print("Error when trying to connect to PostgreSQL:", e)
    finally:
        if conn:
            conn.close()
