import json
import logging
import multiprocessing
import operator
import pathlib
import re
import time
from datetime import datetime

import pandas
import psycopg2
import requests
from bs4 import BeautifulSoup
import project.sample.db_conn as dbc

ROOT_DIR = pathlib.Path(__file__).resolve().parent
DATA_DIR = pathlib.Path(ROOT_DIR.parent, 'data')
DATA_DIR.mkdir(parents=True, exist_ok=True)
FILE_NAME = pathlib.Path(__file__).stem
LOGS_DIR = pathlib.Path(ROOT_DIR.parent, "logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = [
    {
        'category': 'CPU',
        'url': 'www.ldlc.com/informatique/pieces-informatique/processeur/c4300/'
    },
    {
        'category': 'GPU',
        'url': 'www.ldlc.com/informatique/pieces-informatique/carte-graphique-interne/c4684/'
    }
]

""" LOGGER CONFIGURATION """
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

log_file_name = f"{FILE_NAME}_{datetime.now():%Y%m%d_%H%M%S}.log"
log_file_path = pathlib.Path(LOGS_DIR, log_file_name)
file_handler = logging.FileHandler(log_file_path)
logger.addHandler(file_handler)

formatter = logging.Formatter('%(asctime)s :: %(name)s :: %(levelname)-8s :: %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def is_before_update(date):
    """ Checks if date is before update or not. """

    date_dt = datetime.strptime(date[:7], '%Y-%m')
    compare_date_dt = datetime.strptime('2019-02', '%Y-%m')
    return date_dt < compare_date_dt


def get_iso_date(timestamp):
    """ Gets iso-formatted date from timestamp string. """

    iso_timestamp = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}"
    iso_date = datetime.strptime(iso_timestamp, "%Y-%m-%d").date().isoformat()

    return iso_date


def get_soup(url):
    """ Gets soup from url. """

    headers = {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 OPR/93.0.0.0'}

    while True:
        try:
            response = requests.get(url, headers=headers)
            break
        except requests.exceptions.ConnectionError:
            print("requests.exceptions.ConnectionError")
            time.sleep(10)

    return BeautifulSoup(response.content, 'html.parser')


def get_archive_urls(src_url):
    """ Retrieves urls to be scraped using web.archive API. """

    webarchive_cdx = f'http://web.archive.org/cdx/search/cdx?url={src_url}&output=json'

    headers = {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 OPR/93.0.0.0'}
    page = requests.get(webarchive_cdx, headers=headers)
    soup = json.loads(str(BeautifulSoup(page.content, 'html.parser')))

    df = pandas.DataFrame(soup[1:], columns=soup[0])

    return df


def process_df(df, target):
    """ Takes the df containing all URLs to be scraped, adapts it,
    and filters it out to remove already processed URLs. """

    df['date'] = df['timestamp'].apply(lambda x: str(x)[:4]+"-"+str(x)[4:6]+"-"+str(x)[6:8])
    df['category'] = target['category']
    df['url'] = df.apply(lambda row: f'https://web.archive.org/web/{row["timestamp"]}/{row["original"]}', axis=1)
    df['length'] = df['length'].astype(int)

    # Filters out duplicates for dates and keeps only entries for which 'length' is maximum
    idx = df.groupby('date')['length'].idxmax()
    df = df.loc[idx]

    # Retrieves all fully processed URLs from database and removes corresponding entries from daataframe
    proc_url = dbc.get_proc_url()
    proc_url = [(row[0], row[1].strftime('%Y-%m-%d')) for row in proc_url]
    df = df[~df[['category', 'date']].apply(tuple, axis=1).isin(proc_url)]

    # Resets index, very important for chunking to work
    df = df.reset_index()

    return df


def extract_page_prices(soup):
    """ Extracts prices data from HTML patterns using regex. """
    
    page_prices = []

    try:
        price_trails = re.findall(r'''\$\("#pdt-\D{2}\d{12} \.price"\)\.replaceWith\('<div class="price"><div class="price">.+<\/sup><\/div><\/div>'\);''', str(soup))

        for trail in price_trails:
            product_id = re.search(r'#pdt-(\D{2}\d{12}?)', trail).group(1)
            price_info = re.search(r'<div class="price"><div class="price">(.+?)<\/sup>', trail).group(1).split('<sup>')
            integral = ''.join(re.findall(r'\d+', price_info[0]))
            fractional = ''.join(re.findall(r'\d+', price_info[1]))
            product_price = float(integral + '.' + fractional)

            page_prices.append(
                {
                    "product_id": product_id,
                    "product_price": product_price
                }
            )
        
        return page_prices

    except:
        return page_prices


def price_from_string(str):
    """ Uses regex to extract integral and fractional price parts from string. """

    if len(str) == 1:
        temp = re.sub(r'\\xa0', "", str[0])
        price = float(''.join(re.findall(r'[\d.,]+', temp)).replace(',', '.'))
    else:
        integral = ''.join(re.findall(r'\d+', str[0]))
        fractional = ''.join(re.findall(r'\d+', str[1]))
        price = float(integral + '.' + fractional)

    return price


class Product:
    """
    Class to manage Product objects scraped from URL. 

    Attributes:
        category (str): Product category.
        date (str): Date scope.
        sku (str): Product identifier.
        title (str): Product website designation.
        desc (str): Product website description.
        model (str): Product model.
        price (float): Product price.
    
    Methods:
        get_sku(html): Retrieves sku (str) from HTML.
        get_title(html): Retrieves title (str) from HTML.
        get_info(html): Retrieves desc (str) and model (str) from HTML.
        get_price(html): Retrieves price (float) from HTML.
        to_dict(): Returns a dictionary with all attributes.
    """

    def get_sku(self, html):
        if html.select_one('td[class="designation"] > a[class="seemore"]'):
            sku_info = html.select_one('td[class="designation"] > a[class="seemore"]').get('href')
            sku = re.search(r'(\D{2}\d{12}?)', sku_info).group(1)
        else:
            sku = html['data-id']
        
        return sku

    def get_title(self, html):
        if html.select_one('td[class="designation"] > a') is not None:
            title = html.select_one('td[class="designation"] > a').get_text(strip=True)
        elif html.select_one('div[class="pdt-info"] > h3[class="title-3"] > a') is not None:
            title = html.select_one('div[class="pdt-info"] > h3[class="title-3"] > a').get_text(strip=True) # .get('title') || .text.strip()
        elif html.select_one('div[class="dsp-cell-right"] > div > div > h3[class="title-3"]') is not None:
            title = html.select_one('div[class="dsp-cell-right"] > div > div > h3[class="title-3"]').get_text(strip=True)
        
        return title[:120]

    def get_info(self, html):
        if html.select_one('td[class="designation"] > span') is not None:
            html_info = html.select_one('td[class="designation"] > span').get_text(strip=True)
        elif html.select_one('td[class="designation"] > div[class="caract"] > span') is not None:
            html_info = html.select_one('td[class="designation"] > div[class="caract"] > span').get_text(strip=True)
        if html.select_one('div[class="pdt-info"] > h3[class="title-3"] > a') is not None:
            html_info = html.select_one('div[class="pdt-info"] > p[class="desc"]').get_text(strip=True)
        elif html.select_one('div[class="dsp-cell-right"] > div > div > h3[class="title-3"]') is not None:
            html_info = html.select_one('div[class="dsp-cell-right"] > div > div > p[class="desc"]').get_text(strip=True)
            
        desc = re.sub(r'\([^()]*\)', '', html_info).strip()
        try:
            model = re.search(r'\(([^()]+)\)', html_info).group(1)
        except AttributeError:
            model = ''
            # logger.exception(f'PRODUCT :: ERROR :: url:{row["url"]}, page: {i}, index: {index}, model could not be defined, script will resume normally')
        
        return desc[:140], model

    def get_price(self, html, page_prices):
        if is_before_update(self.date):
            if html.select_one('td[class="prix"] > span[class="price"]') is not None:
                price_info = html.select_one('td[class="prix"] > span[class="price"]').prettify().split('<sup>')
                price = price_from_string(price_info)
            
            else:
                price = None

        else:
            if html.select_one('div[class="basket"] > div[class="price"] > div[class="price"]') is not None:
                price_info = html.select_one('div[class="basket"] > div[class="price"] > div[class="price"]').get_text(strip=True)
                price = price_from_string(price_info)

            else: # Meaning price is in the script part of the HTML
                id = html.get('data-id')
                try:
                    index = list(map(operator.itemgetter("product_id"), page_prices)).index(id)
                    price = page_prices[index]["product_price"]
                except ValueError:
                    price = None
        
        return price

    def to_dict(self):
        dict = {
            'sku': self.sku,
            'category': self.category,
            'title': self.title,
            'description': self.desc,
            'model': self.model,
            'price': self.price,
            'date': self.date
        }

        return dict

    def __init__(self, html, row, page_prices):
        self.category = row['category']
        self.date = row['date']
        self.sku = self.get_sku(html)
        self.title = self.get_title(html)
        self.desc, self.model = self.get_info(html)
        self.price = self.get_price(html, page_prices)


def get_page_articles(soup):
    """ Returns a list of articles from HTML code. """

    if soup.select('tr[class*="cmp"]:not([class*="group"])'):
        articles = soup.select('tr[class*="cmp"]:not([class*="group"])')
    elif soup.select('li[class="pdt-item"]'):
        articles = soup.select('li[class="pdt-item"]')
    return articles


def get_page_navigation(soup):
    """ Returns the pagination-related code and the number of pages from HTML code. """

    if soup.find('ul', class_='pagerItems') is not None:
        pagination = soup.find('ul', class_='pagerItems').find_all('li')
        num_pages = len(pagination)
    elif soup.find('ul', class_='pagination') is not None:
        pagination = soup.find('ul', class_='pagination').find_all('li')
        num_pages = len(pagination)
    elif soup.find('ul', class_='pagerUnitItems') is not None:
        pagination = soup.find('ul', class_='pagerUnitItems').find_all('li')
        num_pages = len(pagination)
    else:
        pagination = None
        num_pages = 1
    return pagination, num_pages


def scrape_url(row, db_conn):
    """ Scrapes a URL and add all its products data into database. """

    # Gets the soup
    try:
        soup = get_soup(row["url"])
    except requests.exceptions.InvalidSchema:
        logger.exception(f'URL :: ERROR :: {row["url"]}, invalid schema')
        return

    is_all_scraped = True

    articles = get_page_articles(soup)

    try:
        pagination, num_pages = get_page_navigation(soup)
    except Exception as e:
        logger.exception(f'URL :: ERROR :: {row["url"]}, {e}')
        return

    # Loops through pages
    for i in range(1, num_pages + 1):
        if i != 1:
            new_url = pagination[i-1].find('a').get('href')
            if new_url[:4] != 'http':
                new_url = 'https://web.archive.org' + new_url
            try:
                soup = get_soup(new_url)
            except requests.exceptions.InvalidSchema:
                logger.exception(f'PAGE :: ERROR :: url:{row["url"]}, page: {i}')
                is_all_scraped = False
                continue

            articles = get_page_articles(soup)
        
        page_prices = extract_page_prices(soup)

        # Loops through articles
        for index, article in enumerate(articles):
            try:
                product = Product(article, row, page_prices)
                # Checks if product entry already in database
                if dbc.is_prod_proc(conn=db_conn, date=product.date, sku=product.sku):
                    logger.info(f"PRODUCT :: {product.sku} already processed")
                    continue
                else:
                    dbc.add_data_to_products_tb(conn=db_conn, dict=product.to_dict())
                    logger.info(f"PRODUCT :: {product.to_dict()}")
            except Exception as e:
                logger.exception(f'PRODUCT :: ERROR :: url:{row["url"]}, page: {i}, index: {index}, {e}')
                is_all_scraped = False
    
    # Marks the URL as fully scraped if all products processed
    if is_all_scraped:
        dbc.update_url_row(db_conn, row)


def test():
    """ Checks specific URL for debugging. """

    try:
        dbc.db_init()
    except Exception as e:
        logger.exception(f'ERROR :: {e}')

    db_conn = psycopg2.connect(
            host=dbc.HOST,
            user=dbc.USER,
            password=dbc.PASSWORD,
            port=dbc.PORT,
            database=dbc.DATABASE_NAME,
    )

    url = "https://web.archive.org/web/20181023002455/https://www.ldlc.com/informatique/pieces-informatique/processeur/c4300/"
    date = get_iso_date(re.search(r'\d{14}', url).group(0))
    temp = re.search(r'c4300|c4684', url).group(0)
    if temp == 'c4300':
        category = 'CPU'
    elif temp == 'c4684':
        category = 'GPU'

    row = {
            "url": url,
            "category": category,
            "date": date
    }

    scrape_url(row, db_conn)

    db_conn.close()


def chunk_manager(chunk):
    """ Handles chunk of dataframe containing the URLs to be scraped.
    Each chunk has its own database connection to allow concurrent reading
    and writing operations. """

    for index, row in chunk.iterrows():
        db_conn = psycopg2.connect(
            host=dbc.HOST,
            user=dbc.USER,
            password=dbc.PASSWORD,
            port=dbc.PORT,
            database=dbc.DATABASE_NAME,
        )

        # Creates URL entry if it does not exist already in database
        dbc.init_entry_url_tb(db_conn, row)

        # Checks if URL is fully processed
        if dbc.is_url_proc(db_conn, row):
            pass
        else:
            try:
                scrape_url(row, db_conn)
            except psycopg2.errors.InFailedSqlTransaction:
                logger.exception(f'ERROR :: URL :: {row["url"]}, {row["date"]}, {row["category"]}')
                db_conn.rollback()
            except UnboundLocalError:
                logger.exception(f'ERROR :: URL :: {row["url"]}, {row["date"]}, {row["category"]}')
            except requests.exceptions.ChunkedEncodingError:
                continue

        db_conn.commit()
        db_conn.close()


def main():
    try:
        dbc.db_init()
    except Exception as e:
        logger.exception(f'ERROR :: {e}')

    for target in TARGETS:

        df = get_archive_urls(target["url"])
        # Filters out already processed urls to adapt chunk size
        df = process_df(df, target)

        if int(df.shape[0] / multiprocessing.cpu_count()) >= 1:
            chunk_size = int(df.shape[0] / multiprocessing.cpu_count())
        else:
            chunk_size = 1

        try:
            chunks = [df.iloc[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
            with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                pool.map(chunk_manager, chunks)
        except Exception as e:
            logger.exception(f"ERROR :: CHUNK :: {e}")


if __name__ == '__main__':
    main()