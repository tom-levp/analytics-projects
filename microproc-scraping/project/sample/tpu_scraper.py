import json
import logging
import pathlib
import re
import time, random
from datetime import datetime

import pandas
import numpy
import psycopg2
import project.sample.db_conn as dbc
import requests
from bs4 import BeautifulSoup

import asyncio
from playwright.async_api import async_playwright

ROOT_DIR = pathlib.Path(__file__).resolve().parent
DATA_DIR = pathlib.Path(ROOT_DIR.parent, 'data')
DATA_DIR.mkdir(parents=True, exist_ok=True)
FILE_NAME = pathlib.Path(__file__).stem
LOGS_DIR = pathlib.Path(ROOT_DIR.parent, "logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = ['cpu', 'gpu']

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


def load_data(file_name):
    """ Loads data from JSON file. """

    file_path = pathlib.Path(DATA_DIR, f'{file_name}.json')
    check = file_path.is_file()

    if check is True:
        with open(file_path, 'r', encoding="utf8") as file:
            data = json.load(file)
    elif check is not True:
        with file_path.open("w", encoding="UTF-8") as path: 
            data = []
            json.dump(data, path, indent=4, ensure_ascii=False)

    return data


def save_data(retrieved_data, file_name):
    """ Saves data in JSON and CSV formats. """

    json_path = pathlib.Path(DATA_DIR, f"{file_name}.json")

    # Write JSON files:
    with json_path.open("w", encoding="UTF-8") as target: 
        json.dump(retrieved_data, target, indent=4, ensure_ascii=False)


def remove_ordinals(string):    
    """ Removes ordinal characters from string using regex. """

    return re.sub(r'(\d)(st|nd|rd|th)', r'\1', string)


def to_isodate(date_string):
    """ Converts date string to iso format. """

    try:
        return datetime.strptime(remove_ordinals(date_string), '%b %d, %Y').date().isoformat()
    except ValueError:
        return datetime.strptime(date_string, '%b %Y').date().isoformat()


def get_unique_keys(dict_list):
    """ Takes a list of dictionary and returns a list of all their unique keys. """

    # # Get all unique keys
    # unique_keys = set().union(*data)
    # unique_keys = list({key for d in data for key in d})

    # Gets all unique keys in the order they appear
    unique_keys = []
    for dict in dict_list:
        for key in dict.keys():
            if key not in unique_keys:
                unique_keys.append(key)


async def scrape():
    """ Scrapes TechPowerUp CPU and GPU data. """

    for target in TARGETS:

        file_path = pathlib.Path(DATA_DIR, f'{target}_whole_specs.json')
        check = file_path.is_file()

        if check is True:
            with open(file_path, 'r', encoding="utf8") as file:
                unique_products = json.load(file)
        elif check is not True:
            with file_path.open("w", encoding="UTF-8") as path: 
                unique_products = dbc.get_table_as_records(target)
                json.dump(unique_products, path, indent=4, ensure_ascii=False)

        url = f'https://www.techpowerup.com/{target}-specs/?sort=name'

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            await page.set_viewport_size(viewport_size={"width": 1920, "height": 1080})

            try:
                for element in unique_products:
                    try:
                        if element['status'] == 'DONE':
                            continue
                    except KeyError:
                        await page.goto(url)
                        await page.wait_for_selector('input[id="quicksearch"]')
                        await page.wait_for_timeout(1000)
                        await page.type('input[id="quicksearch"]', element['model'])

                        try:
                            if target == 'cpu':
                                await page.wait_for_selector('div[class="tablewrapper"] > table > tbody > tr', timeout=(random.randint(8, 14) * 1000))
                            elif target == 'gpu':
                                await page.wait_for_selector('div[id="ajaxresults"] > table > tbody > tr', timeout=(random.randint(8, 14) * 1000))
                        except:
                            element['status'] = 'DONE'
                            continue
                        else:
                            if target == 'cpu':
                                items = await page.locator('div[class="tablewrapper"] > table > tbody > tr > td > a').all()
                            elif target == 'gpu':
                                items = await page.locator('div[id="ajaxresults"] > table > tbody > tr > td > a').all()
                                print(len(items))

                            await page.wait_for_timeout(random.randint(8, 14) * 1000)
                            await items[0].click()

                            # Wait for the next page to load
                            await page.wait_for_selector('div[class="sectioncontainer"]')

                            if target == 'cpu':
                                sections = await page.locator('section[class="details"] > table > tbody').all()
                            elif target == 'gpu':
                                sections = await page.locator('section[class="details"] > div').all()

                            for section in sections:
                                if target == 'cpu':
                                    rows = await section.locator('tr').all()
                                elif target == 'gpu':
                                    rows = await section.locator('dl').all()

                                for row in rows:
                                    if target == 'cpu':
                                        try:
                                            await page.wait_for_selector('th', timeout=250)
                                            label_co = await row.locator('th').inner_text()
                                            content = await row.locator('td').inner_text()
                                            label = label_co[:-1]
                                        except:
                                            continue
                                    elif target == 'gpu':
                                        label_co = await row.locator('dt').inner_text()
                                        content = await row.locator('dd').inner_text()
                                        label = label_co
                                    element['status'] = 'DONE'
                                    element[label] = content
                                logger.info(element)

                            await page.wait_for_timeout(random.randint(8, 14) * 1000)
                            continue
            except Exception as e:
                logger.exception(e)
            finally:
                save_data(unique_products, f"{target}_whole_specs")

            browser.close()


###################################################################


def clean_dataset(df, target):
    """ Applies a number of transformations to dataframe. """

    if target == 'cpu':
        df = df[['model', 'Process Size', 'Transistors', 'Die Size', 'Launch Price', 'Release Date', '# of Cores', '# of Threads', 'Frequency', 'TDP', 'Foundry']]
        
        df['Process Size'] = df['Process Size'].apply(lambda x: int(x.replace(' nm', '')) if isinstance(x, str) else x)
        df['Transistors'] = df['Transistors'].apply(lambda x: int(x.replace(' million', '').replace(',', '')) if isinstance(x, str) else x)
        df['Die Size'] = df['Die Size'].apply(lambda x: x.replace(' mm²', '') if isinstance(x, str) else x)
        df['Die Size'] = df['Die Size'].apply(lambda x: int(x.split('x')[0]) * int(x.split('x')[1]) if isinstance(x, str) and 'x' in x else x)
        df['Launch Price'] = df['Launch Price'].apply(lambda x: int(x.replace('$', '')) if isinstance(x, str) else x)
        df['Release Date'] = df['Release Date'].apply(lambda x: to_isodate(x) if isinstance(x, str) else x)
        df['Frequency'] = df['Frequency'].apply(lambda x: float(x.replace(' GHz', '')) if isinstance(x, str) and 'GHz' in x else (float(x.replace(' MHz', '')) / 1000 if isinstance(x, str) and 'MHz' in x else x))
        df['TDP'] = df['TDP'].apply(lambda x: int(x.replace(' W', '')) if isinstance(x, str) else x)
        df['Foundry'] = df['Foundry'].apply(lambda x: x.upper() if isinstance(x, str) else x)

        df = df.replace(['nan', 'NaN', numpy.nan], [None, None, None])

        df['Process Size'] = df['Process Size'].astype('str')
        df['Transistors'] = df['Transistors'].astype('str')
        df['Die Size'] = df['Die Size'].astype('str')
        df['Launch Price'] = df['Launch Price'].astype('str')
        df['Release Date'] = df['Release Date'].astype('str')
        df['Frequency'] = df['Frequency'].astype('str')
        df['TDP'] = df['TDP'].astype('str')
        df['Foundry'] = df['Foundry'].astype('str')

        df = df.rename(columns={
            'model': 'model',
            'Process Size': 'process_size_nm',
            'Transistors': 'transistor_count',
            'Die Size': 'die_size_mm2',
            'Launch Price': 'launch_price_usd',
            'Release Date': 'release_date',
            '# of Cores': 'core_count',
            '# of Threads': 'thread_count',
            'Frequency': 'frequency_ghz',
            'TDP': 'tdp_w',
            'Foundry': 'foundry'
        })
    
    elif target == 'gpu':
        df = df[['model', 'Architecture', 'Process Size', 'Transistors', 'Density', 'Die Size', 'TDP', 'Memory Size', 'Memory Type', 'Launch Price', 'Release Date', 'Tensor Cores', 'Pixel Rate', 'Texture Rate', 'FP32 (float)', 'Base Clock', 'Boost Clock', 'Foundry']]
    
        df['Architecture'] = df['Architecture'].apply(lambda x: x.upper() if isinstance(x, str) else None)
        df['Process Size'] = df['Process Size'].apply(lambda x: int(x.replace(' nm', '')) if isinstance(x, str) else numpy.nan)
        df['Transistors'] = df['Transistors'].apply(lambda x: int(x.replace(' million', '').replace(',', '')) if isinstance(x, str) else numpy.nan)
        df['Density'] = df['Density'].apply(lambda x: float(x.replace('M / mm²', '')) if isinstance(x, str) and 'M / mm²' in x else (float(x.replace('K / mm²', '')) / 1000 if isinstance(x, str) and 'K / mm²' in x else numpy.nan))
        df['Die Size'] = df['Die Size'].apply(lambda x: int(x.replace(' mm²', '')) if isinstance(x, str) else numpy.nan)
        df['TDP'] = df['TDP'].apply(lambda x: x.replace(' W', '').replace('unknown', '') if isinstance(x, str) else x)
        df['Memory Size'] = df['Memory Size'].apply(lambda x: float(x.replace(' GB', '')) if isinstance(x, str) and 'GB' in x else (float(x.replace(' MB', '')) / 1024 if isinstance(x, str) and 'MB' in x else numpy.nan))
        df['Launch Price'] = df['Launch Price'].apply(lambda x: int(x.replace(' USD', '').replace(',', '')) if isinstance(x, str) else numpy.nan)
        df['Release Date'] = df['Release Date'].apply(lambda x: numpy.nan if x == 'Never Released' else (to_isodate(x) if isinstance(x, str) else x))
        df['Pixel Rate'] = df['Pixel Rate'].apply(lambda x: float(x.replace(' GPixel/s', '')) if isinstance(x, str) else numpy.nan)
        df['Texture Rate'] = df['Texture Rate'].apply(lambda x: float(x.replace(' GTexel/s', '').replace(',', '')) if isinstance(x, str) and 'GTexel/s' in x else (float(x.replace(' MTexel/s', '').replace(',', '')) / 1000 if isinstance(x, str) and 'MTexel/s' in x else numpy.nan))
        df['FP32 (float)'] = df['FP32 (float)'].apply(lambda x: float(x.replace(' TFLOPS', '').replace(',', '')) if isinstance(x, str) and 'TFLOPS' in x else (float(x.replace(' GFLOPS', '').replace(',', '')) / 1000 if isinstance(x, str) and 'GFLOPS' in x else numpy.nan))
        df['Base Clock'] = df['Base Clock'].apply(lambda x: int(x.replace(' MHz', '')) if isinstance(x, str) else numpy.nan)
        df['Boost Clock'] = df['Boost Clock'].apply(lambda x: int(x.replace(' MHz', '')) if isinstance(x, str) else numpy.nan)
        df['Foundry'] = df['Foundry'].apply(lambda x: x.upper() if isinstance(x, str) else None)

        df[['Architecture', 'Release Date', 'Foundry', 'Memory Type']] = df[['Architecture', 'Release Date', 'Foundry', 'Memory Type']].replace(['nan', 'NaN', numpy.nan], None)
        df[['Architecture', 'Release Date', 'Foundry', 'Memory Type']] = df[['Architecture', 'Release Date', 'Foundry', 'Memory Type']].astype('str')

        df[['Process Size', 'Transistors', 'Die Size', 'TDP', 'Launch Price', 'Base Clock', 'Boost Clock']] = df[['Process Size', 'Transistors', 'Die Size', 'TDP', 'Launch Price', 'Base Clock', 'Boost Clock']].replace(['', 'nan', 'NaN'], numpy.nan)
        print(df[['Process Size', 'Transistors', 'Die Size', 'TDP', 'Launch Price', 'Base Clock', 'Boost Clock']].to_markdown())
        df[['Process Size', 'Transistors', 'Die Size', 'TDP', 'Launch Price', 'Base Clock', 'Boost Clock']] = df[['Process Size', 'Transistors', 'Die Size', 'TDP', 'Launch Price', 'Base Clock', 'Boost Clock']].astype('Int64')

        df[['Density', 'Memory Size', 'Pixel Rate', 'Texture Rate', 'FP32 (float)']] = df[['Density', 'Memory Size', 'Pixel Rate', 'Texture Rate', 'FP32 (float)']].astype('Float64')
        df[['Density', 'Memory Size', 'Pixel Rate', 'Texture Rate', 'FP32 (float)']] = df[['Density', 'Memory Size', 'Pixel Rate', 'Texture Rate', 'FP32 (float)']].replace(['', 'nan', 'NaN'], numpy.nan)

        df = df.rename(columns={
            'model': 'model',
            'Architecture': 'architecture',
            'Process Size': 'process_size_nm',
            'Transistors': 'transistor_count',
            'Density': 'density_m_per_mm2',
            'Die Size': 'die_size_mm2',
            'TDP': 'tdp_w',
            'Memory Size': 'memory_size_gb',
            'Memory Type': 'memory_type',
            'Launch Price': 'launch_price_usd',
            'Release Date': 'release_date',
            'Tensor Cores': 'tensor_core_count',
            'Pixel Rate': 'pixel_rate_gpixel_per_s',
            'Texture Rate': 'texture_rate_gtexel_per_s',
            'FP32 (float)': 'fp32_tflops',
            'Base Clock': 'base_clock_mhz',
            'Boost Clock': 'boost_clock_mhz',
            'Foundry': 'foundry'
        })

    return df


def push_df_to_db(df, target):
    """ Pushes dataframe content to Postgres tables. """

    db_conn = psycopg2.connect(
        host=dbc.HOST,
        user=dbc.USER,
        password=dbc.PASSWORD,
        port=dbc.PORT,
        database=dbc.DATABASE_NAME,
    )

    data = df.to_dict(orient="records")
    data = [{k: None if pandas.isna(v) or v == 'None' or v == '' else v for k, v in d.items()} for d in data]

    for row_dict in data:
        if target == 'cpu':
            dbc.add_data_to_cpu_specs_tb(db_conn, row_dict)
        elif target == 'gpu':
            dbc.add_data_to_gpu_specs_tb(db_conn, row_dict)

    db_conn.commit()
    db_conn.close()


def transform_load():
    """ Initiates the Postgres tables and feed them with clean data. """

    dbc.create_specs_tables()
    for target in TARGETS:
        data = load_data(f'{target}_whole_specs')

        # Convert the list of dictionaries to a pandas DataFrame
        df = pandas.DataFrame(data)
        df = clean_dataset(df, target)

        push_df_to_db(df, target)


if __name__ == '__main__':
    asyncio.run(scrape())
    # transform_load()