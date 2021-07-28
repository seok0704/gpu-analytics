from requests import get
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
from json import loads
from IPython.core.display import clear_output
import psycopg2


# Update connection string information 
host = ""
dbname = "postgres"
user = ""
password = ""
sslmode = "require"
# Construct connection string
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

def connect_postgreSQL(conn_string):
    
    conn = psycopg2.connect(conn_string) 
    print("successfully Connected")
    
    return conn, conn.cursor()

def run_query(conn,query):
    return pd.read_sql(query, con=conn)

def insert(conn, cursor, insert_query):
    cursor.execute(insert_query)
    conn.commit()


#Scrape Chipsets_list
def scrape_chipsets(conn_string):
    
    conn, cursor = connect_postgreSQL(conn_string)
    
    response = get(url='https://pcpartpicker.com/products/video-card/')
    html_soup = BeautifulSoup(response.text, 'html.parser')
    
    
    for gpu_class in html_soup.find_all("li", {"class": "abbreviated_c"}):
        gpu = gpu_class.find('label').get_text()
                
        insert_query = "INSERT INTO chipsets(chipset_name) VALUES ('{}')".format(gpu)
        
        try:        
            insert(conn, cursor, insert_query)
        except Exception as e:
            conn.rollback()
            print("Fail to insert the chipset: {}".format(gpu))
            
    print("Chipset Data Uploaded")

#Scrape GPU Card Info

def insert_card_info(card_id,html_soup,conn, cursor,chipset_data):
    
    chipset_exist = True
    
    def get_specs(specs_block, content):
        try:
            return specs_block.find(text=content).find_parent('h3').next_sibling.next_sibling.text.strip()
        except Exception as e:
            return 'null'
        
    name = html_soup.find('h1', class_='pageTitle').text
    
    try:
        rating = float(html_soup.find('ul', class_='product--rating list-unstyled').next_sibling.strip().split()[2])
    except Exception as e:
        rating = 'null'

    specs_block = html_soup.find('div', class_='block xs-hide md-block specs')
    
    
    chipset_name = get_specs(specs_block,'Chipset')
    
    try:
        chipset_id = chipset_data[chipset_data['chipset_name']==chipset_name]['chipset_id'].iloc[0]
        
        manufacturer = get_specs(specs_block,'Manufacturer')
        memory = get_specs(specs_block,'Memory')
        core_clock = get_specs(specs_block,'Core Clock')
        boost_clock = get_specs(specs_block,'Boost Clock')
        color = get_specs(specs_block,'Color')
        length = get_specs(specs_block,'Length')

        insert_query = '''
                INSERT INTO gpu_info(
                    card_id,
                    name,
                    chipset_id,
                    rating,
                    manufacturer,
                    memory,
                    core_clock,
                    boost_clock,
                    color,
                    length
                ) 
                VALUES ('{}', '{}', {}, {}, '{}', '{}','{}', '{}', '{}','{}')
                ON CONFLICT(card_id) DO NOTHING
            '''.format(card_id, name, chipset_id, rating, manufacturer, memory, core_clock, boost_clock, color, length)
        
    except Exception as e:
        print("Cannot find the chipset_id for card_id: {}".format(card_id))
        chipset_exist = False
        return chipset_exist
        
    try:        
        insert(conn, cursor, insert_query)
        
    except Exception as e:
        conn.rollback()
        print("Fail to insert the chipset: {}".format(card_id))
    
    return chipset_exist

#Insert Price  
def insert_price(card_id,html_soup,conn, cursor):        
    scripts = html_soup.findAll('script')
    
    for s in scripts:
        if "var chart_data = " in s.prettify():
            data = loads(s.prettify().split("var chart_data = ")[1].rsplit(';')[0])
        else:
            data = []
            
    if len(data) == 0:
        print("No price for {}".format(card_id))
        return 
    
    else:
        for merch in data:
            merchant_name = merch['label']
            
            for data_point in merch['data']:
                datetime = data_point[0]
                price = data_point[1]
                
                if datetime and price:
                    
                    insert_query = '''
                            INSERT INTO card_prices(
                                card_id,
                                datetime,
                                merchant_name,
                                price
                            ) 
                            VALUES ('{}', {}, '{}', {})
                        '''.format(card_id, int(datetime/1000), merchant_name, float(price/100))

                    try:        
                        insert(conn, cursor, insert_query)
                    except Exception as e:
                        conn.rollback()
                        print("Fail to insert the price for the card_id: {}".format(card_id))
                        
def scrape_card_info(conn_string):
    conn, cursor = connect_postgreSQL(conn_string)
    
    chipset_data = run_query(conn,("select * from chipsets"))
    
    curr_data = (run_query(conn,("select * from gpu_info")))
    
    if len(curr_data)>=0:
        chipset_data = chipset_data[~chipset_data['chipset_id'].isin(curr_data['chipset_id'].unique())]
    
    for chipset in chipset_data['chipset_name']:
        page = 1
        print("Working")
        print("----------------Working on chipset: {}------------".format(chipset))
        while True:
            sleep(10.0)
            card_ids = []
            
            headers = {'User-Agent': 'for personal project, contact:seok0704@gmail.com'}

            response = get(url='https://pcpartpicker.com/search/?q={}&page={}'.format(chipset,page),headers = headers)
            html_soup = BeautifulSoup(response.text, 'html.parser')
                
            gpu_id_htmls = html_soup.find_all("p", {"class": "search_results--link"})
            
            if len(gpu_id_htmls)==0:
                break
            
            else:
                for gpu_class in html_soup.find_all("p", {"class": "search_results--link"}):
                    gpu = gpu_class.find_all('a', href=True)
                    card_id = str(gpu[0]).split('/')[2]
                    card_ids.append(card_id)
                    
                for card_id in card_ids:
                    print("Inserting card ID: {} to the database".format(card_id))
                    sleep(10.0)
                    headers = {'User-Agent': 'for personal project, contact:seok0704@gmail.com'}

                    response = get(url='https://pcpartpicker.com/product/{}/?history_days=365'.format(card_id),headers = headers)
                    html_soup = BeautifulSoup(response.text, 'html.parser')

                    chipset_exist = insert_card_info(card_id,html_soup,conn, cursor,chipset_data)
                    if chipset_exist:
                        insert_price(card_id,html_soup,conn, cursor)
                        
                page +=1

def scrape_benchmark(conn_string):
    conn, cursor = connect_postgreSQL(conn_string)
    chipset_data = run_query(conn,("select * from chipsets"))

    insert_query = '''
            INSERT INTO gpu_benchmark(
                chipset_id,
                msrp_price,
                value_for_money,
                score,
                popularity
            ) 
            VALUES ({}, {}, {}, {}, {})
            ON CONFLICT(chipset_id) DO NOTHING
            '''

    def get_chipset_id(chipset_name,chipset_data):
        
        chipset_name = chipset_name.split()[-2] + " " + chipset_name.split()[-1]
        return chipset_data[chipset_data['chipset_name'].str.lower().str.replace("[()\\s-]+", " ").str.contains(chipset_name.lower())]['chipset_id'].iloc[0]

    def get_content(row):
        chipset_name = row[0].find('a').text
        chipset_id = get_chipset_id(chipset_name,chipset_data)

        if row[1].text.strip() == 'n/a': 
            msrp_price = 'null' 
        else: 
            msrp_price = int(row[1].text.strip()[1:])    
            
        score = int(row[2].text.strip())
        
        if row[3].text.strip() == 'n/a':
            value_for_money = 'null'
        else: 
            value_for_money = int(row[3].text.strip())    
            
            
        popularity = float(row[4].text.strip())

        return chipset_id,msrp_price,value_for_money,score,popularity

    
    headers = {'User-Agent': 'for personal project, contact:seok0704@gmail.com'}

    response = get(url='https://benchmarks.ul.com/compare/best-gpus?amount=0&sortBy=SCORE&reverseOrder=true&types=DESKTOP&minRating=0',headers = headers)
    html_soup = BeautifulSoup(response.text, 'html.parser')   
    
    row_1 = html_soup.findChildren("td")[1:7]
    row_2 = html_soup.findChildren("td")[7:12]
    
    for row in [row_1,row_2]:
        chipset_id,msrp_price,value_for_money,score,popularity = get_content(row)
        insert(conn, cursor, insert_query.format(chipset_id,msrp_price,value_for_money,score,popularity))

    for row in html_soup.findChildren("tr")[3:]:
        print(row.find_all('td')[1:][0].find('a').text)
        try:
            row = row.find_all('td')[1:]
            chipset_id,msrp_price,value_for_money,score,popularity = get_content(row)       


            insert(conn, cursor, insert_query.format(chipset_id,msrp_price,value_for_money,score,popularity))
            print("Chipset_id: {} Success!",format(chipset_id))

        except Exception as e:
            
            print("chipset_name: {} Failed!",format(chipset_id))

if __name__=='__main__':
    scrape_chipsets(conn_string)
    scrape_card_info(conn_string)
    scrape_benchmark(conn_string)