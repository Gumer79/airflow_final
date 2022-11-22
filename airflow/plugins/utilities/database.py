from random import randint
import re

import psycopg2
from bs4 import BeautifulSoup
from fake_headers import Headers
import time
from datetime import date
from unicodedata import normalize
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from airflow.hooks.base import BaseHook
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_db_url(connector_id: str) -> str:
    connection = BaseHook.get_connection(connector_id)

    return f'user={connection.login} password={connection.password} host={connection.host} ' \
           f'port={connection.port} dbname={connection.schema}'


def create_ads_table(connector_id: str):
    connection = psycopg2.connect(get_db_url(connector_id))
    with connection.cursor() as curs:
        curs.execute("""CREATE TABLE IF NOT EXISTS ads(
                ad_id bigserial PRIMARY KEY NOT NULL,
                name varchar(50),
                price numeric,
                publication_date varchar(50),
                parsing_date date,
                stage varchar(50),
                year integer,
                mileage integer,
                beaten varchar(15),
                engine_power varchar(20),
                engine_vol varchar(15),
                gas_type varchar(30),
                transmission varchar(30), 
                gear varchar(30),
                car_body varchar(30),
                color varchar(30),
                wheel varchar(30),
                n_owners varchar(5), 
                metro_st varchar(255),
                Latitude float,
                Longitude float,
                href varchar(255),
                kmeans smallint);""")
        connection.commit()
        print("[INFO] Table created successfully")


def insert_ads_at_table(connector_id: str, data: dict):
    connection = psycopg2.connect(get_db_url(connector_id))
    query = """INSERT INTO ads (
                ad_id,
                name,
                price,
                metro_st,
                publication_date,
                parsing_date,
                stage,
                year,
                mileage,
                beaten,
                engine_power,
                engine_vol,
                gas_type,
                transmission, 
                gear,
                car_body,
                color,
                wheel,
                n_owners, 
                Latitude,
                Longitude,
                href
                 ) VALUES (
                %(ad_id)s,
                %(name)s,
                %(price)s,
                %(metro_st)s,
                %(publication_date)s,
                %(parsing_date)s,
                %(stage)s,
                %(year)s,
                %(mileage)s,
                %(beaten)s,
                %(engine_power)s,
                %(engine_vol)s,
                %(gas_type)s,
                %(transmission)s, 
                %(gear)s,
                %(car_body)s,
                %(color)s,
                %(wheel)s,
                %(n_owners)s, 
                %(Latitude)s,
                %(Longitude)s,
                %(href)s)
                """

    with connection.cursor() as curs:
        curs.execute(query, data)

        connection.commit()

        print(f"[INFO] {data['ad_id']} is inserted successfully.")


def check_ad_in_db(connector_id: str, ad_id: str):
    """ перед началом парсинга сайта с объявлением выполняет проверку наличия этого объявления в базе данных """
    connection = psycopg2.connect(get_db_url(connector_id))
    with connection.cursor() as curs:
        curs.execute(f"""select exists(SELECT ad_id FROM ads WHERE ad_id={ad_id});""")
        return curs.fetchone()[0]


def get_data_from_ad(source) -> dict:
    """get page source and return ad data"""
    result_data = {}
    soup = BeautifulSoup(source, 'lxml')
    # ad id
    result_data['ad_id'] = soup.find('div', class_='style-item-map-wrapper-ElFsX')['data-item-id']
    # car name
    result_data['name'] = soup.find("span", {"class": "title-info-title-text"}).text.split(',')[0]
    # price
    result_data['price'] = soup.find('span', class_='js-item-price')['content']

    try:
        # publication date
        result_data['publication_date'] = normalize('NFKD', soup.find_all(class_='text-text-LurtD text-size-s-BxGpL')[
            2].text).replace('·', '').strip()
    except AttributeError:
        result_data['publication_date'] = None
    # parsing date
    result_data['parsing_date'] = date.today().strftime("%Y.%m.%d")
    # количество параметров в блоке с параметрами(в каждом объявлении они отличаются, поэтому парсится по наименованию требуемого параметра)
    len_cars_parameters_blok = len(soup.find_all('li', class_='params-paramsList__item-appQw'))
    try:
        # car stage
        result_data['stage'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1)
             if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Поколение'][0]
    except IndexError:
        result_data['stage'] = None
    try:
        # car build year
        result_data['year'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1)
             if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Год выпуска'][0]
    except IndexError:
        result_data['year'] = None
    try:
        # mileage
        result_data['mileage'] = [re.findall('^[0-9]+',
                                             soup.find_all('li', class_='params-paramsList__item-appQw')[
                                                 i].text.replace('\n',
                                                                 '').split(
                                                 ':')[1].strip())[0] for i in range(0, len_cars_parameters_blok - 1) if
                                  soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n',
                                                                                                              '').strip().split(
                                      ':')[0] == 'Пробег'][0]
    except IndexError:
        result_data['mileage'] = None
    try:
        # beaten/not beaten
        result_data['beaten'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1) if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Состояние'][0]
    except IndexError:
        result_data['beaten'] = None
    try:
        # engine power
        result_data['engine_power'] = [re.findall('^.*?\([^\d]*(\d+)[^\d]*\).*$',
                                                  soup.find_all('li', class_='params-paramsList__item-appQw')[
                                                      i].text.replace('\n',
                                                                      '').split(
                                                      ':')[1].strip())[0] for i in
                                       range(0, len_cars_parameters_blok - 1) if
                                       soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n',
                                                                                                                   '').strip().split(
                                           ':')[0] == 'Модификация'][0]
    except IndexError:
        result_data['engine_power'] = None
    try:
        # engine volume
        result_data['engine_vol'] = [re.findall('\d\.\d',
                                                soup.find_all('li', class_='params-paramsList__item-appQw')[
                                                    i].text.replace('\n',
                                                                    '').split(
                                                    ':')[1].strip())[0] for i in range(0, len_cars_parameters_blok - 1)
                                     if
                                     soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n',
                                                                                                                 '').strip().split(
                                         ':')[0] == 'Объём двигателя'][0]
    except IndexError:
        result_data['engine_vol'] = None
    try:
        # gas type
        result_data['gas_type'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1) if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Тип двигателя'][0]
    except IndexError:
        result_data['gas_type'] = None
    try:
        # the transmissin type
        result_data['transmission'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1) if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Коробка передач'][0]
    except IndexError:
        result_data['transmission'] = None
    try:
        # the gear type
        result_data['gear'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1) if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Привод'][0]
    except IndexError:
        result_data['gear'] = None
    try:
        # the type of body
        result_data['car_body'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1) if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Тип кузова'][0]
    except IndexError:
        result_data['car_body'] = None
    try:
        # the color of body
        result_data['color'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1) if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Цвет'][0]
    except IndexError:
        result_data['color'] = None
    try:
        # the side of wheel
        result_data['wheel'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1) if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Руль'][0]
    except IndexError:
        result_data['wheel'] = None
    try:
        # the number of owners
        result_data['n_owners'] = \
            [soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').split(':')[1].strip()
             for i in range(0, len_cars_parameters_blok - 1) if
             soup.find_all('li', class_='params-paramsList__item-appQw')[i].text.replace('\n', '').strip().split(':')[
                 0] == 'Владельцев по ПТС'][0]
    except IndexError:
        result_data['n_owners'] = None
    try:
        # if the car is new, then the number of owners and mileage will be 0, а состаяние не битый
        if soup.find('span',
                     class_='style-newLabel-DBypj').text == 'Новый':
            result_data['n_owners'] = 0
            result_data['mileage'] = 0
            result_data['beaten'] = 'Не битый'
    except AttributeError:
        pass

    try:
        # the subway station
        result_data['metro_st'] = soup.find(
            class_='style-item-address-georeferences-item-icons-_Zkh_').find_next_sibling().text
    except AttributeError:
        # if station is none, then parsing the street name
        result_data['metro_st'] = soup.find(class_='style-item-address__string-wt61A').text.strip()

    try:
        # latitude and longitude
        get_location = soup.find('div', class_='style-item-map-wrapper-ElFsX')
        result_data['Latitude'] = get_location['data-map-lat']
        result_data['Longitude'] = get_location['data-map-lon']
    except IndexError:
        result_data['Latitude'] = None
        result_data['Longitude'] = None

    try:
        # link to ad
        result_data['href'] = soup.find('meta', {'property': "og:url"})['content']
    except IndexError:
        result_data['href'] = None

    print(f"[INFO] {result_data['ad_id']} is successfully parsed.")
    return result_data


def load_data_into_db(connector_id: str, selenium_hub_ip: str):
    """ launch selenium on chrome driver and save data on database """
    # generate random headers
    header = Headers(
        browser="random",  # Generate random UA
        os="random",  # Generate ony random platform
        headers=True  # generate misc headers
    )
    options = Options()
    options.add_argument("--headless")  # Работа браузера в фоновом режиме
    options.add_argument(header.generate()['User-Agent'])
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")

    hub = f"http://{selenium_hub_ip}:4444/wd/hub"
    print(f'driver going to connect on {hub}')
    driver = webdriver.Remote(hub, options=options)
    url = f'https://www.avito.ru/moskva/avtomobili?cd=1&radius=0&s=104'
    driver.get(url)
    print("driver has been started successfully")
    # print(driver.page_source)
    max_page = int(
        driver.find_element(By.XPATH, '//*[@id="app"]/div/div[4]/div[3]/div[3]/div[5]/div[1]/span[8]').text)
    for page_counter, page in enumerate(range(max_page)):
        url = f'https://www.avito.ru/moskva/avtomobili?cd=1&p={page}&radius=0&s=104'
        driver.get(url)
        print(f'page {page_counter + 1}/{max_page}')
        ad_items = driver.find_elements(By.XPATH, '//*[@id]/div/div[1]/a')[:-1]
        for ad_counter, ad_item in enumerate(range(len(ad_items))):
            print(f'ad {ad_counter + 1}/{len(ad_items)}; page {page_counter + 1}/{max_page}')
            id_before_parsing = ad_items[ad_item].get_attribute('href').split("_")[-1]
            if not check_ad_in_db(connector_id, id_before_parsing):
                try:
                    time.sleep(randint(3, 6))
                    WebDriverWait(driver, 20).until(EC.element_to_be_clickable(ad_items[ad_item])).click()
                    print('clicked')
                    driver.switch_to.window(driver.window_handles[1])
                    print('switched')
                    page_source = driver.page_source
                    print('sourced')
                    data = get_data_from_ad(page_source)
                    insert_ads_at_table(connector_id, data)
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                except TypeError:
                    driver.refresh()
                    page_source = driver.page_source
                    print('sourced')
                    try:
                        data = get_data_from_ad(page_source)
                        insert_ads_at_table(connector_id, data)
                    except:
                        pass
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                except Exception as ex:
                    print(ex)
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                    print(f"[INFO] couldn't load the ad")
            else:
                print(f'[INFO] {id_before_parsing} is already in database.')
    print('All pages have been parsed successfully.')


def load_data_into_db2():
    """ launch selenium on chrome driver and save data on database """
    # generate random headers
    header = Headers(
        browser="random",  # Generate random UA
        os="random",  # Generate ony random platform
        headers=True  # generate misc headers
    )
    options = Options()
    options.add_argument("--headless")  # Работа браузера в фоновом режиме
    options.add_argument(header.generate()['User-Agent'])
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    print('going to...')
    hub = f"http://0.0.0.0:4444/wd/hub"
    print(f'driver going to connect on {hub}')
    driver = webdriver.Remote(hub, options=options)
    url = f'https://www.avito.ru/moskva/avtomobili?cd=1&radius=0&s=104'
    driver.get(url)
    print("driver has been started successfully")
    print(driver.page_source)
    max_page = int(
        driver.find_element(By.XPATH, '//*[@id="app"]/div/div[4]/div[3]/div[3]/div[5]/div[1]/span[8]').text)
    print(max_page)
    # for page_counter, page in enumerate(range(max_page)):
    #     url = f'https://www.avito.ru/moskva/avtomobili?cd=1&p={page}&radius=0&s=104'
    #     driver.get(url)
    #     print(f'page {page_counter + 1}/{max_page}')
    #     ad_items = driver.find_elements(By.XPATH, '//*[@id]/div/div[1]/a')[:-1]
    #     for ad_counter, ad_item in enumerate(range(len(ad_items))):
    #         print(f'ad {ad_counter + 1}/{len(ad_items)}; page {page_counter + 1}/{max_page}')
    #         id_before_parsing = ad_items[ad_item].get_attribute('href').split("_")[-1]
    #         if not check_ad_in_db(connector_id, id_before_parsing):
    #             try:
    #                 time.sleep(randint(3, 6))
    #                 # ad_items[ad_item].click()
    #                 WebDriverWait(driver, 20).until(EC.element_to_be_clickable(ad_items[ad_item])).click()
    #                 print('clicked')
    #                 driver.switch_to.window(driver.window_handles[1])
    #                 print('switched')
    #                 page_source = driver.page_source
    #                 print('sourced')
    #                 data = get_data_from_ad(page_source)
    #                 insert_ads_at_table(connector_id, data)
    #                 driver.close()
    #                 driver.switch_to.window(driver.window_handles[0])
    #             except TypeError:
    #                 driver.refresh()
    #                 page_source = driver.page_source
    #                 print('sourced')
    #                 try:
    #                     data = get_data_from_ad(page_source)
    #                     insert_ads_at_table(connector_id, data)
    #                 except:
    #                     pass
    #                 driver.close()
    #                 driver.switch_to.window(driver.window_handles[0])
    #             except Exception as ex:
    #                 print(ex)
    #                 driver.close()
    #                 driver.switch_to.window(driver.window_handles[0])
    #                 print(f"[INFO] couldn't load the ad")
    #         else:
    #             print(f'[INFO] {id_before_parsing} is already in database.')
    # print('All pages have been parsed successfully.')

# load_data_into_db2()