import json
import logging
import pathlib
import re
import random
from datetime import datetime

import asyncio
from playwright.async_api import async_playwright
from fake_useragent import UserAgent

ROOT_DIR = pathlib.Path(__file__).resolve().parent
DATA_DIR = pathlib.Path(ROOT_DIR.parent, 'data')
DATA_DIR.mkdir(parents=True, exist_ok=True)
FILE_NAME = pathlib.Path(__file__).stem
LOGS_DIR = pathlib.Path(ROOT_DIR.parent, "logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = ['cpu', 'gpu']
COUNTRIES = [
    {'country_code': 'FR', 'country_name': 'France'},
    {'country_code': 'GB', 'country_name': 'United Kingdom'},
    {'country_code': 'RO', 'country_name': 'Romania'},
    {'country_code': 'PL', 'country_name': 'Poland'},
    {'country_code': 'IN', 'country_name': 'India'},
    {'country_code': 'JP', 'country_name': 'Japan'},
    {'country_code': 'US', 'country_name': 'United States'}
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


def save_data(retrieved_data, file_name):
    """ Saves data in JSON and CSV formats. """

    json_path = pathlib.Path(DATA_DIR, f"{file_name}.json")

    # write JSON files:
    with json_path.open("w", encoding="UTF-8") as target: 
        json.dump(retrieved_data, target, indent=4, ensure_ascii=False)


async def get_proxies_pw():
    """ Scrapes the Proxynova website to retrieve a list of proxies. """

    async with async_playwright() as p:
        ua = UserAgent()
        user_agent = ua.chrome

        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent=user_agent)
        page = await context.new_page()
        await page.set_viewport_size(viewport_size={"width": 1920, "height": 1080})        

        proxy = {}
        u = list()

        for country in COUNTRIES:
            url = f'https://www.proxynova.com/proxy-server-list/country-{country["country_code"]}/'

            await page.goto(url)
            await page.wait_for_selector('table[id="tbl_proxy_list"]')
            
            rows = await page.locator('table[id="tbl_proxy_list"] > tbody > tr').all()
            for row in rows:
                cells = await row.locator('td').all()
                
                elite_check = await cells[-1].locator('span').get_attribute('class')

                if elite_check.startswith('proxy_elite'):
                    try: 
                        proxy['ip'] = re.search(r'\)(\d+\.\d+\.\d+\.\d+)', await cells[0].text_content()).group(1)
                    except Exception as e:
                        proxy['ip'] = None
                    try:
                        proxy['port'] = re.search(r'(\d+)', await cells[1].text_content()).group(1)
                    except Exception as e:
                        proxy['port'] = None
                    proxy['country'] = country['country_name']
                    if proxy['port'] is not None:
                        u.append(proxy)
                        logger.info(f'New proxy added: {proxy}')
                    proxy={}
                else:
                    continue

            await page.wait_for_timeout(random.randint(8, 20) * 1000)
        
        save_data(u, 'proxies')


if __name__ == '__main__':
    asyncio.run(get_proxies_pw())