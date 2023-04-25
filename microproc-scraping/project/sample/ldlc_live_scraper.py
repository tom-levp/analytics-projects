from playwright.sync_api import Playwright, sync_playwright
import re, datetime, json, pathlib
import pandas as pd

today = datetime.date.today()

ROOT_DIR = pathlib.Path(__file__).resolve().parent

SCRAPING_URL = 'https://www.ldlc.com/informatique/pieces-informatique/carte-graphique-interne/c4684/'

def save_data(retrieved_data, fileName):
    """ Saves data in JSON and CSV formats. """

    json_path = pathlib.Path(f"{ROOT_DIR}\\data\\output", f"{fileName}.json")
    csv_path = pathlib.Path(f"{ROOT_DIR}\\data\\output", f"{fileName}.csv")

    # Writes JSON files:
    with json_path.open("w", encoding="UTF-8") as target: 
        json.dump(retrieved_data, target, indent=4, ensure_ascii=False)

    # Writes CSV files:
    df = pd.read_json(json_path, encoding='utf-8-sig')
    df.to_csv(csv_path, index = None, encoding='utf-8-sig')


def main():

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_viewport_size(viewport_size={"width": 1920, "height": 1080})

        page.goto(SCRAPING_URL)

        pagination = page.locator('ul.pagination li').all()
        num_pages = len(pagination) - 1

        list = []

        for i in range(1, num_pages + 1):
            if i != 1:
                page.goto(SCRAPING_URL + f"page{i}")
            else:
                pass

            # Get all articles on current page
            articles = page.locator('li.pdt-item').all() # .query_all()
            for article in articles:
                article_data = article.locator('div.dsp-cell-right')

                title = article_data.locator('h3.title-3').text_content().strip()

                article_info = article_data.locator('p.desc').text_content().strip()
                desc = re.sub(r'\([^()]*\)', '', article_info).strip()
                try:
                    model = re.search(r'\(([^()]+)\)', article_info).group(1)
                except AttributeError:
                    model = None

                availability = article_data.locator('div[class^="modal-stock-web pointer stock stock"]').text_content().strip()

                price_info = article_data.locator('div.price div.price').inner_html().strip().split('<sup>')
                integral = price_info[0][:-1].replace('&nbsp;', '')
                fractional = re.search(r'\d+', price_info[1]).group(0)
                price = float(integral + '.' + fractional)

                list.append({
                    'title': title,
                    'desc': desc,
                    'model': model,
                    'price': price,
                    'date': today.isoformat()
                })

        browser.close()
    
    save_data(list, 'data')


if __name__ == '__main__':
    main()