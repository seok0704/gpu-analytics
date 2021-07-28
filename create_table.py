from requests import get
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
from json import loads
from IPython.core.display import clear_output
import psycopg2



create_chipset_database = """
CREATE TABLE chipsets(
    chipset_id SERIAL PRIMARY KEY,
    chipset_name TEXT); 
"""
create_gpu_cards = """
CREATE TABLE gpu_info(
    card_id VARCHAR(250) PRIMARY KEY,
    name VARCHAR(250),
    chipset_id INTEGER,
    rating FLOAT,
    manufacturer VARCHAR(250),
    memory VARCHAR(50),
    core_clock VARCHAR(50),
    boost_clock VARCHAR(50),
    color VARCHAR(50),
    length VARCHAR(50),
    FOREIGN KEY(chipset_id) REFERENCES chipsets(chipset_id)
); 
"""

create_card_prices = """
CREATE TABLE card_prices(
    card_id VARCHAR(250),
    datetime INT,
    merchant_name VARCHAR(250),
    price FLOAT,
    PRIMARY KEY(card_id, merchant_name, datetime),
    FOREIGN KEY(card_id) REFERENCES gpu_info(card_id)
    ); 
"""

create_benchmark = """
CREATE TABLE gpu_benchmark(
    chipset_id INTEGER PRIMARY KEY,
    msrp_price INTEGER,
    value_for_money INT,
    score INT,
    popularity float,
    FOREIGN KEY(chipset_id) REFERENCES chipsets(chipset_id)
); 
"""

def connect_postgreSQL():
    # Update connection string information 
    host = ""
    dbname = "postgres"
    user = ""
    password = ""
    sslmode = "require"
    # Construct connection string
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

    conn = psycopg2.connect(conn_string) 
    print("successfully Connected")
    
    return conn, conn.cursor()

def create_table(table_query):  
    conn, cursor = connect_postgreSQL()

    conn.cursor().execute(create_chipset_database)
    conn.commit()

if __name__=='__main__':
    create_table(create_chipset_database)
    create_table(create_gpu_cards)
    create_table(create_card_prices)
    create_table(create_benchmark)