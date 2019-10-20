###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Обработка HTML
from bs4 import BeautifulSoup
# Для генерации поддельного User agent
from fake_useragent import UserAgent
import urllib
ua = UserAgent()
# Для работы с запросами
import requests
from requests import ConnectTimeout, ConnectionError, ReadTimeout
# Для работы с браузером
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, InvalidSessionIdException, WebDriverException

# Для работы с табличными данными
import pandas as pd

# Для работы с регулярными выражениями
import re

# Для работы с массивами и вычислениями
import numpy as np

# Для работы с SQL
import sqlalchemy
from sqlalchemy import create_engine

# Для работы с операционной системой
import os

# Для работы с циклами
from itertools import cycle

# Создадим подключние к dwh
engine = create_engine('postgres://volokzhanin:{password}@localhost:5432/volokzhanin'.format(password = os.getenv('PASSWORD1', False)))

# Получаем прокси сервера
proxy_df = pd.read_sql(
    con = engine,
    sql = """
    select 
            name 
    from 
            staging_tables.proxy_servers
    where 
            is_work = True
    """
)

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################
class geocoder:
    def __init__(
            self,
            city_address = 'Приморский край, Владивосток',
            proxy_cycle = cycle(proxy_df.name)
    ):
        """
        Функция для инциализации объекта класса.
        Параметры:
        city_address - текстовое наименование города поиска,
        proxy_cycle - зацикленные прокси-адреса.
        """
        self.city_address = city_address
        self.proxy_cycle = proxy_cycle

    def yandex_city_coordinates(self) -> list():
        """
        Функция для получения широты и долготы адреса с yandex карт.
        Вход: текст адреса.
        Выход: список широты и долготы.
        """
        while True:
            try:
                url = 'https://yandex.ru/maps/75/vladivostok/?text={text}'.format(text=urllib.parse.quote(self.city_address))
                chrome_options = webdriver.ChromeOptions()
                # Вставляем прокси
                chrome_options.add_argument('--proxy-server={proxy}'.format(proxy=next(self.proxy_cycle)))
                # Вставляем user agent
                chrome_options.add_argument("user-agent={user_agent}".format(user_agent=ua.Chrome))
                #  Запускаем без графического драйвера
                chrome_options.add_argument('--headless')
                # Исправляем ошибку DevToolsActivePort
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                driver = webdriver.Chrome(options = chrome_options)
                # Установим time out
                driver.implicitly_wait(10)
                driver.get(url)
                wait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, '._view_full')))
                driver.find_element(By.CSS_SELECTOR, '.card-title-view')
                page_source = driver.page_source
                bsObj = BeautifulSoup(page_source, 'html5lib')
                coordinats_list = bsObj.find_all('div', {'class': 'clipboard__action-wrapper _inline'})
                # Ищем координтаы
                if len(coordinats_list) > 0:
                    latitude, longitude = re.findall(r'\d{1,3}.\d{1,10}', coordinats_list[0].text)
                else:
                    latitude, longitude = None, None
                return [latitude, longitude]
                driver.close()
                break
            except WebDriverException as error:
                continue
                driver.close()

    def coordinates(self, address, city_latitude = 43.115536, city_longitude = 131.885485) -> pd.DataFrame():
        """
        Функция для получения широты и долготы адреса с yandex карт.
        Вход: текст адреса, широта города, долгота города.
        Выход: таблица: адрес, широта и долгота.
        """
        while True:
            try:
                proxies = next(self.proxy_cycle)
                headers = ua.Chrome
                url = 'https://yandex.ru/maps/75/vladivostok/?text={text}'.format(text = urllib.parse.quote(address))
                chrome_options = webdriver.ChromeOptions()
                # Вставляем прокси
                chrome_options.add_argument('--proxy-server={proxy}'.format(proxy = proxies))
                # Вставляем user agent
                chrome_options.add_argument("user-agent={user_agent}".format(user_agent = headers))
                #  Запускаем без графического драйвера
                chrome_options.add_argument('headless')
                driver = webdriver.Chrome(options = chrome_options)
                # Установим time out
                driver.implicitly_wait(10)
                driver.get(url)
                # Ждем загрузки левой панели
                wait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, '._view_full')))
                # Ищем плашку с координатами
                driver.find_element(By.CSS_SELECTOR, '.card-title-view')
                page_source = driver.page_source
                bsObj = BeautifulSoup(page_source, 'html5lib')
                coordinats_list = bsObj.find_all('div', {'class' : 'clipboard__action-wrapper _inline'})
                # Ищем координтаы
                if len(coordinats_list) > 0:
                    latitude, longitude = re.findall(r'\d{1,3}.\d{1,10}', coordinats_list[0].text)
                else:
                    latitude, longitude = None, None
                if latitude == city_latitude and longitude == city_longitude:
                    latitude, longitude = None, None
                else:
                    latitude = latitude
                    longitude = longitude
                return pd.DataFrame({'address' : address, 'latitude' : [latitude], 'longitude' : [longitude], 'geom' : [None]})
                break
            except NoSuchElementException:
                return pd.DataFrame({'address': address, 'latitude': [None], 'longitude': [None], 'geom': [None]})
            except TimeoutException:
                continue
            finally:
                driver.close()