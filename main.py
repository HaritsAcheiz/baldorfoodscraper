import re
from dataclasses import dataclass
from dotenv.main import load_dotenv
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.webdriver import WebDriver
from httpx import AsyncClient, Client, Cookies
from urllib.parse import urljoin
import asyncio
import sqlite3
import pandas as pd
import os
from selectolax.parser import HTMLParser
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import ElementClickInterceptedException

load_dotenv()

@dataclass
class BFScraper:
    cookies: Cookies = None
    base_url: str = 'https://www.baldorfood.com/'
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    api_base_url: str= 'https://www.baldorfood.com/api/v1/products'


    # Support Custom Function
    def extract_price(self, input_string):
        pattern = r'\$\d+\.\d{2}'
        match = re.search(pattern, input_string)
        if match:
            return match.group(0)  # Return the matched string

        return None


    def extract_number(self, input_string):
        # pattern = r'#tab-(\d+)'
        pattern = r'(\d+)'
        match = re.search(pattern, input_string)
        if match:
            return match.group(1)

        return None


    # Preparation
    def webdriver_setup(self):
        ff_opt = Options()
        ff_opt.add_argument('-headless')
        ff_opt.add_argument('--no-sandbox')
        ff_opt.set_preference("general.useragent.override", self.user_agent)
        ff_opt.page_load_strategy = 'eager'
        driver = WebDriver(options=ff_opt)

        return driver


    def get_cookies(self):
        url = urljoin(self.base_url, '/users/default/new-login')
        driver = self.webdriver_setup()
        driver.maximize_window()
        driver.get(url)
        wait = WebDriverWait(driver, 20)

        # login
        creds = os.getenv('BALDOREMAIL') + Keys.TAB + os.getenv('BALDORPASSWORD') + Keys.RETURN
        wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, 'input#EmailLoginForm_email'))).send_keys(creds)
        wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR, 'div.loginbox.user-menu.js-user-menu')))
        cookies = driver.get_cookies()
        httpx_cookies = Cookies()
        for cookie in cookies:
            httpx_cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
        self.cookies = httpx_cookies
        driver.close()


    # First Version Async HTML
    def get_category_url(self):
        headers = {
            'user-agent': self.user_agent
        }

        with Client(headers=headers) as client:
            response = client.get(self.base_url)
        if response.status_code != 200:
            response.raise_for_status()

        tree = HTMLParser(response.text)
        category_element = tree.css_first('ul.catalog-categories.foods-menu')
        categories = category_element.css('a.menu-fi-item')
        category_urls = list()
        for category in categories:
            category_urls.append(urljoin(self.base_url, category.attributes.get('href', '')))

        return category_urls


    async def fetch(self, aclient, url, proxy, limit):
        headers = {
            'user-agent': self.user_agent,
        }

        sel_proxy = {
            "http://": f"http://{proxy}",
            "https://": f"http://{proxy}"
        }

        async with limit:
            aclient.cookies.update(self.cookies)
            response = await aclient.get(url)
            print(url, response)
            if limit.locked():
                await asyncio.sleep(1)
            if response.status_code != 200:
                response.raise_for_status()

        return url, response.text


    async def fetch_all(self, urls):
        tasks = []
        proxies = os.getenv('ROYALPROXIES').split(',')
        proxy_index = 0
        headers = {
            'user-agent': self.user_agent,
        }
        limit = asyncio.Semaphore(4)

        async with AsyncClient(headers=headers, timeout=120) as aclient:
            for url in urls:
                if proxy_index > 19:
                    proxy_index = 0
                task = asyncio.create_task(self.fetch(aclient, url=url, limit=limit, proxy=proxies[proxy_index]))
                tasks.append(task)
                proxy_index += 1

            htmls = await asyncio.gather(*tasks)

        return htmls


    # Second Version Sync HTML
    def sync_fetch(self, url):
        headers = {
            'User-Agent': self.user_agent,
        }

        with Client(headers=headers) as client:
            response = client.get(url)
            if response.status_code != 200:
                response.raise_for_status()
            print(url, response)
            result = (url, response.text)

        return result

    def sync_fetch_all(self, urls):
        htmls = [self.sync_fetch(url) for url in urls]
        return htmls


    def insert_to_db(self, htmls):
        if os.path.exists('baldorfood.db'):
            os.remove('baldorfood.db')
        conn = sqlite3.connect("baldorfood.db")
        curr = conn.cursor()
        curr.execute(
            """
            CREATE TABLE IF NOT EXISTS products_src(
            url TEXT,
            html BLOB
            ) 
            """
        )

        for html in htmls:
            curr.execute(
                "INSERT INTO products_src (url, html) VALUES(?,?)",
                html)
            conn.commit()

    def get_subcategory_url(self, htmls):
        subcategory_urls = list()
        for html in htmls:
            print(html[1])
            tree = HTMLParser(html[1])
            # subcategory_element = tree.css_first('div.subcats-list-mode')
            # subcategories = subcategory_element.css('a.subcat-l-photo')
            for subcategory in subcategories:
                subcategory_urls.append(urljoin(self.base_url, subcategory.attributes.get('href', '')) + '?viewall=1')

        return subcategory_urls

    def get_product_url(self, htmls):
        product_urls = list()
        for html in htmls:
            tree = HTMLParser(html[1])
            product_element = tree.css_first('div.items')
            products = product_element.css('div.js-cover-container.js_product_card.product-card-table.pct-diff-buttons.pct-light.inter')
            for product in products:
                product_urls.append(urljoin(self.base_url, product.css_first('a[unbxdattr="product"]').attributes.get('href', '')))

        return product_urls

    def get_data(self):
        conn = sqlite3.connect("baldorfood.db")
        curr = conn.cursor()
        curr.execute("SELECT url, html FROM  products_src")
        datas = curr.fetchall()
        product_datas = list()
        for data in datas:
            current_product = dict()
            tree = HTMLParser(data[1])
            elements = ['Farm', 'Title', 'SKU', 'Price', 'Price Unit', 'About Product', 'Ingredient', 'Farm Location', 'About Farm']

            farm_elm = ('Farm' ,tree.css_first('span.card-detail-farm'))
            title_elm = ('Title', tree.css_first('h1.card-details-title'))
            sku_elm = ('SKU', tree.css_first('div.card-detail-sku'))
            price_elm = ('Price', tree.css_first('span.price'))
            price_unit_elm = ('Price Unit', tree.css_first('span.price-unit'))
            about_product_elm = ('About Product', tree.css_first('div.product-note > div.mce-content'))
            ingredient_elm = ('Ingredient', tree.css_first('div#productIngredient'))
            farm_loc_elm = ('Farm Address', tree.css_first('div.farm-descr-box > div.pn-heading.clearfix > strong.pn-title > span'))
            about_farm_elm = ('About Farm', tree.css_first('div.farm-descr-box > div.clearfix.mce-content'))
            elms = [farm_elm, title_elm, sku_elm, price_elm, price_unit_elm, about_product_elm, ingredient_elm, farm_loc_elm, about_farm_elm]

            for elm in elms:
                if elm[1]:
                    current_product[elm[0]] = elm[1].text(strip=True)
                    if elm[0] == 'Price':
                        current_product[elm[0]] = self.extract_price(current_product[elm[0]])
                else:
                    current_product[elm[0]] = ''

            product_datas.append(current_product)

        image_df = pd.DataFrame.from_records(product_datas)
        if not os.path.exists('./result'):
            os.mkdir('./result')
        image_df.to_csv('result/products.csv', index=False)


    # Third Version Async hidden API
    def get_category_ids2(self):
        url = urljoin(self.base_url, '/users/default/new-login')
        driver = self.webdriver_setup()
        driver.maximize_window()
        driver.get(url)
        wait = WebDriverWait(driver, 15)

        # login
        creds = os.getenv('BALDOREMAIL') + Keys.TAB + os.getenv('BALDORPASSWORD') + Keys.RETURN
        wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, 'input#EmailLoginForm_email'))).send_keys(creds)
        wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR, 'div.loginbox.user-menu.js-user-menu')))
        cookies = driver.get_cookies()
        httpx_cookies = Cookies()
        for cookie in cookies:
            httpx_cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
        self.cookies = httpx_cookies
        category_elems = wait.until(ec.presence_of_all_elements_located((By.CSS_SELECTOR, 'ul.nav.nav- > li > a')))
        category_ids = list()
        for elem in category_elems:
            category_ids.append(self.extract_number(elem.get_attribute('class')))

        # for i in range(1, len(category_elems)):
        #     wait.until(ec.presence_of_all_elements_located((By.CSS_SELECTOR, 'ul.nav.nav- > li > a')))
        #     category_id = self.extract_number(driver.find_element(By.CSS_SELECTOR, f'ul.nav.nav- > li:nth-of-type({i}) > a').get_attribute('class'))
        #     clickable = False
        #     while not clickable:
        #         try:
        #             wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR, f'ul.nav.nav- > li:nth-of-type({i}) > a'))).click()
        #             clickable = True
        #         except:
        #             driver.implicitly_wait(3)
        #     count_of_product = 0
        #     while count_of_product == 0:
        #         count_of_product = int(self.extract_number(wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, 'div.category-grid__results'))).text.strip()))
        #         driver.implicitly_wait(3)
        #     category_ids.append((category_id, count_of_product))
        #     driver.back()

        driver.close()

        return category_ids

    def get_category_ids(self):
        headers = {
            'user-agent': self.user_agent
        }

        with Client(headers=headers) as client:
            response = client.get(self.base_url)
        if response.status_code != 200:
            response.raise_for_status()

        tree = HTMLParser(response.text)
        category_element = tree.css_first('ul.catalog-categories.foods-menu')
        categories = category_element.css('a.menu-fi-item')
        category_ids = list()
        for category in categories:
            category_ids.append(self.extract_cat_id(category.attributes.get('data-href', '')))

        return category_ids


    async def fetch_data(self, aclient, cat_id, limit, proxy):
        url = urljoin(self.api_base_url, f'?filter=category eq {cat_id}&page[number]=0&page[size]=2000')
        headers = {
            'user-agent': self.user_agent,
        }

        sel_proxy = {
            "http://": f"http://{proxy}",
            "https://": f"http://{proxy}"
        }

        async with limit:
            aclient.cookies.update(self.cookies)
            response = await aclient.get(url)
            print(url, response)
            if limit.locked():
                await asyncio.sleep(1)
            if response.status_code != 200:
                response.raise_for_status()

        return response.json()


    async def fetch_all_data(self, category_ids):
        tasks = []
        proxies = os.getenv('ROYALPROXIES').split(',')
        proxy_index = 0
        headers = {
            'user-agent': self.user_agent,
        }
        limit = asyncio.Semaphore(4)

        async with AsyncClient(headers=headers, timeout=120) as aclient:
            for cat_id in category_ids:
                if proxy_index > 19:
                    proxy_index = 0
                task = asyncio.create_task(self.fetch_data(aclient, cat_id=cat_id, limit=limit, proxy=proxies[proxy_index]))
                tasks.append(task)
                proxy_index += 1

            json_responses = await asyncio.gather(*tasks)

        return json_responses

    def get_data2(self, json_datas):
        results = list()
        for json_data in json_datas:
            print(json_data)
            print(json_data['data'])
            for data in json_data['data']:
                product = dict()
                product['provider'] = data['attributes']['provider']
                product['farm_url'] = data['attributes']['farmUrl']
                product['size'] = data['attributes']['size']
                product['title'] = data['attributes']['title']
                product['description'] = data['attributes']['description']
                product['is_local'] = data['attributes']['isLocal']
                product['is_organic'] = data['attributes']['isOrganic']
                product['is_peak_season'] = data['attributes']['isPeakSeason']
                product['unit'] = data['attributes']['unitPricesArray'][0]['unit']
                product['price'] = data['attributes']['unitPricesArray'][0]['price']
                product['brunit'] = data['attributes']['unitPricesArray'][0]['brunit']
                product['max_qty'] = data['attributes']['unitPricesArray'][0]['maxQty']
                image_datas = data['attributes']['images']
                image_list = list()
                for image_data in image_datas:
                    image_list.append(image_data['big'])
                product['is_buyable'] = data['attributes']['isBuyable']
                product['is_available'] = data['attributes']['isAvailable']
                product['product_url'] = data['attributes']['productUrl']
                results.append(product)

        return results


if __name__ == '__main__':
    scraper = BFScraper()
    # scraper.get_cookies()
    # categories = scraper.get_category_url()
    category_ids = scraper.get_category_ids2()
    json_datas = asyncio.run(scraper.fetch_all_data(category_ids))
    results = scraper.get_data2(json_datas)
    records = pd.DataFrame.from_records(results)
    records.to_csv('records.csv', index=False)

    # categories_htmls = scraper.sync_fetch_all(categories[0:1])
    # categories_htmls = asyncio.run(scraper.fetch_all(categories))
    # subcategories = scraper.get_subcategory_url(categories_htmls)

    # subcategories_htmls = scraper.sync_fetch_all(subcategories)
    # subcategories_htmls = asyncio.run(scraper.fetch_all(subcategories))
    # products = scraper.get_product_url(subcategories_htmls)

    # products_htmls = scraper.sync_fetch_all(products)
    # products_htmls = asyncio.run(scraper.fetch_all(products))
    # scraper.insert_to_db(products_htmls)

    # scraper.get_data()
