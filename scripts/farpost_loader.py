###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Обработка HTML
from bs4 import BeautifulSoup
# Для генерации поддельного User agent
from fake_useragent import UserAgent
# Для работы с HTTP-запросами
import requests
from requests import ConnectTimeout, ConnectionError, ReadTimeout
from requests.exceptions import ProxyError
import urllib

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

# Для работы с математическими вычислениями
import math

# Для параллельной работы кода
from multiprocessing.dummy import Pool as ThreadPool

# Для произведения синтаксического анализа (лемматизации)
import pymorphy2 as pm
# Загрузим словарь русского языка
morph = pm.MorphAnalyzer()

# Для работы со временем
import datetime

# Создаем строку подключения к postgre
engine = create_engine('postgres://volokzhanin:{password}@localhost:5432/volokzhanin'.format(
                password=os.getenv('PASSWORD1', False)))
# Получаем прокси сервера
proxy_df = pd.read_sql(
    con=engine,
    sql="""
         select 
                 name 
         from 
                 staging_tables.proxy_servers
         where 
                 is_work = True
         """
)
# Создадим зацикливавние по прокси серверам
proxy_cycle = cycle(proxy_df.name)

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################
class farpost_loader:
    def __init__(
            self,
            url = 'https://www.farpost.ru/vladivostok/realty/sell_flats',
            engine = engine

    ):
        """
        Функция для инциализации объекта класса.
        Параметры:
        url - url для получения объявлений, engine - строка подключения к dwh.
        """
        self.url = url
        self.engine = engine

    def clean_text(self, text):
        """
        Функция для очистки текста
        Параметры: text - текст
        Фозвращаемое значение:
        clean_text - очищенный текст
        """
        # Переводим в нижний регистр
        lower_text = text.lower()
        # Заменяем все кроме буквы или цифры
        clean_text = re.sub(r'\W', ' ', lower_text)
        # Удаляем все пробелы, кроме между словами
        clean_text = ' '.join(clean_text.split())
        return clean_text

    def lem_text(self, text):
        """
        Функция для лемматизации текста
        Параметры:
        text - очищенный текст
        Фозвращаемое значение:
        finish_text - лемматизированный тест
        """
        # Лемматизируем каждое слово
        word_lem = [morph.parse(item)[0].normal_form for item in text.split()]
        # Склеиваем слово через пробел
        finish_text = ' '.join(word_lem)
        return (finish_text)

    def my_session(self, url, headers, proxies, session, timeout=50):
        """
        Функция для возвращения ссессии пользователя с заходом на url.
        Параметры: url - строка url, headers - заголовки, proxies - прокси сервер, session - сессия пользователя.
        Выход - сессия пользователя с заходом на указанную страницу.
        """
        return session.get(
            url,
            headers=headers,
            proxies=proxies,
            timeout=timeout
        )

    def number_pages(self, url, headers, proxies, session):
        """
        Функция для возварщения количества страниц.
        Параметры: url - url, headers - заголовки, proxies - прокси сервер, session - сессия пользователя.
        Выход - список с количеством страниц и предложений.
        """
        url_offers = self.my_session(url, headers, proxies, session)
        bsObj_offers = BeautifulSoup(url_offers.text, 'html5lib')
        count_offers = bsObj_offers.find("span", {"class": "item itemsCount"}).text
        pages = math.ceil(int(re.sub('\D', '', count_offers)) / 50)
        return [int(re.sub('\D', '', count_offers)), pages]

    def pages_all(self):
        """
        Функция для возварщения количества страниц.
        Параметры: нет.
        Выход - список с количеством страниц и предложений.
        """

        # Получаем общее количество предложений и страниц
        while True:
            try:
                headers = {'User-Agent': UserAgent().chrome}
                proxies = {'https': 'https://' + next(proxy_cycle)}
                session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(max_retries=1)
                session.mount('https://', adapter)
                offers, pages = self.number_pages(
                    self.url,
                    headers=headers,
                    proxies=proxies,
                    session=session
                )
                break
            except ConnectTimeout:
                continue
        return [offers, pages]

    def link_ad(self, url, page, proxies, headers, session):
        """
        Функция для получения ссылок на предложения Farpost
        Параметры: url - путь с запросом, page - страница, headers - заголовки, proxies - прокси сервер, session - сессия пользователя.
        Выход: result_df - таблица с предложениями на выбранной странице
        """
        # Перейдем на страницу  и укажем прокси-сервера
        url_links = self.my_session(
            url,
            proxies=proxies,
            headers=headers,
            session=session
        )

        # Приведем текст к понятному виду BeautifulSoup
        bsObj_offers = BeautifulSoup(url_links.text, 'html5lib')

        # Определим маску поиска ссылки
        regex = re.compile('/vladivostok/realty/sell_flats/.+\d{5,10}.html')
        links = bsObj_offers.find_all("a")

        # Создадим объект для сбора результата
        links_list = []

        # Обойдем циклом все ссылки и оставим только необходимые
        for j in links:
            current_link = j.get('href')
            if current_link is None:
                continue
            elif regex.match(current_link) is None:
                continue
            else:
                link = regex.match(current_link)
                links_list.append(link.string)

                # Оставим только уникальные ссылки
        result_df = pd.DataFrame({'raw_url': links_list})
        result_df.drop_duplicates(
            keep='first',
            inplace=True
        )

        # Обработаем данные
        result_df['url'] = result_df.apply(lambda x: 'https://www.farpost.ru' + x['raw_url'], axis=1)
        result_df['page'] = url
        return result_df

    def link_ad_all(self, pages, url='https://www.farpost.ru/vladivostok/realty/sell_flats'):
        """
        Функция для получения таблицы обхода farpost.
        Вход: количество страниц.
        Выход: таблица для обхода.
        """
        while True:
            try:
                headers = {'User-Agent': UserAgent().chrome}
                proxies = {'https': 'https://' + next(proxy_cycle)}
                session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(max_retries=5)
                session.mount('https://', adapter)
                # Создаем объект для сбора результата
                link_ad_df = pd.DataFrame()

                # Пройдемся циклом по всем страницам запроса и соберем все ссылки
                for page in range(1, pages + 1):
                    current_url = url + '/?page={page}'.format(
                        page=page
                    )
                    current_table = self.link_ad(
                        url=current_url,
                        proxies=proxies,
                        headers=headers,
                        session=session,
                        page=page
                    )
                    link_ad_df = pd.concat([link_ad_df, current_table])
                break
            except ConnectTimeout:
                continue

        link_ad_df.reset_index(drop=True, inplace=True)
        return link_ad_df

    def clean_ad(self, text):
        """
        Функция для очистки текста объявления.
        Вход: сырой текст.
        Выход: очищенный тескст.
        """
        tamplate = re.compile('\n|\t| во Владивостоке|Подробности о доме|Адрес|Этаж')
        clean_text = ' '.join(tamplate.sub(' ', text).split()).strip()
        return clean_text

    def address_ad(self, text_block):
        """
        Функция для получения адреса объявления.
        Вход: текст для извлечения адреса.
        Выход: адрес объявления.
        """
        raw_address = re.findall('Адрес[\t\n\r]+.+', text_block)
        if len(raw_address) > 0:
            address = 'Россия Приморский край Владивосток ' + self.clean_ad(raw_address[0]).replace(',', '')
        else:
            address = None
        return address

    def title_ad(self, bsObj_object):
        """
        Функция для получения заголовка объявления.
        Вход: beautiful soup объект.
        Выход: заголовок объявления.
        """
        title = bsObj_object.find_all('h1', {'class': 'subject viewbull-field__container'})
        if len(title) > 0:
            title = self.clean_ad(title[0].text)
        else:
            title = ''
        return title

    def image_ad(self, bsObj_object):
        """
        Функция для получения изображений объявления.
        Вход: beautiful soup объект.
        Выход: лист изображений объявления.
        """
        image = bsObj_object.find_all('img')
        if len(image) > 0:
            image_list = []
            for im in image:
                current_image = re.findall(r'v/\d{1,100}_bulletin', str(im))
                if len(current_image) > 0:
                    image_list.append('https://static.baza.farpost.ru/' + current_image[0])
        else:
            image_list = None
        return image_list

    def price_ad(self, bsObj_object):
        """
        Функция для получения цены в объявлении.
        Вход: beautiful soup объект.
        Выход: цена в объявлении.
        """
        price = bsObj_object.find_all('span', {'class': 'viewbull-summary-price__value'})
        if len(price) > 0:
            price = price[0].text
            price = re.sub('≈|\s', '', price)
            price = re.findall('\d{1,}₽', price)[0]
            price = int(re.sub('₽', '', price))
        else:
            price = None
        return price

    def status_house_ad(self, text_block):
        """
        Функция для получения статуса дома.
        Вход: текст для извлечения статуса дома.
        Выход: статуса дома.
        """
        is_house_delivered = re.findall('Этап строительства дома[\t\n\r]+Не сдан', text_block)
        if len(is_house_delivered) > 0:
            is_house_delivered = 0
        else:
            is_house_delivered = 1
        return is_house_delivered

    def area_ad(self, text_block):
        """
        Функция для получения площади в объявлении.
        Вход: текст для извлечения площади в объявлении.
        Выход: площадь в объявления.
        """
        area = re.findall('Площадь по документам[\t\n\r]+.+', text_block)
        if len(area) > 0:
            area = int(re.findall(r'\d{1,4}', area[0])[0])
        else:
            area = None
        return area

    def is_mortage_ad(self, text_block):
        """
        Функция для получения статуса ипотеки в объявлении.
        Вход: текст для извлечения статуса ипотеки в объявлении.
        Выход: статуса ипотеки в объявлении.
        """
        is_mortage = re.findall('Подходит под ипотеку', text_block)
        if len(is_mortage) > 0:
            is_mortage = 1
        else:
            is_mortage = 0
        return is_mortage

    def floor_ad(self, text_block):
        """
        Функция для получения этажа в объявления.
        Вход: текст для извлечения этажа в объявлении..
        Выход: этаж в объявления.
        """
        floor = re.findall('Этаж[\t\n\r]+.+', text_block)
        if len(floor) > 0:
            floor = self.clean_ad(floor[0])
        else:
            floor = None
        return floor

    def text_ad(self, bsObj_object):
        """
        Функция для получения текста объявления.
        Вход: текст для извлечения текста объявления.
        Выход: текста объявления.
        """
        text = self.clean_ad(bsObj_object.text)
        tamplate_search = re.compile(r"""
        одходит\sпод\sипотеку\s.+.contacts__actions\s{\smargin-right:\s50%;\smargin-bottom:\s10px;\s}
        |Дом [не]*\s*сдан\s.+.contacts__actions\s{\smargin-right:\s50%;\smargin-bottom:\s10px;\s}
        |Состояние\sи\sособенности\sквартиры\s.+.contacts__actions\s{\smargin-right:\s50%;\smargin-bottom:\s10px;\s} 
        |Состояние\sи\sособенности\sквартиры\s.+\s.company-logo
        |Состояние\sи\sособенности\sквартиры\s.+\$\(function
        """, re.VERBOSE)
        text = tamplate_search.findall(text)
        tamplate_delete = re.compile(r"""
        Не\sподходит\sпод\sипотеку\s|Подходит\sпод\sипотеку\s
        |\s.contacts__actions\s{\smargin-right:\s50%;\smargin-bottom:\s10px;\s}
        |\$\(function.+|Дом\s*[не]*сдан\s
        |Состояние\sи\sособенности\sквартиры\s
        |\s.company-logo
        |\$\(function
        """, re.VERBOSE)
        if len(text) > 0:
            text = tamplate_delete.sub('', text[0]).strip()
        else:
            text = None
        return text

    def is_balcony(self, text):
        """
        Функция для получения наличия балкона.
        Вход: очищенный текст.
        Выход: 1 - есть балкон, 0 - нет балкона.
        """
        if len(re.findall('балкон', text)) > 0:
            result = 1
        else:
            result = 0
        return result

    def is_builder_ad(self, bsObj_object, text_block):
        """
        Функция для возвращения принадлежности объявления к застройщику.
        Вход: bsObj_object - beautiful soup объект, text_block - текстовый блок.
        Выход: 0 - не принадлежит застройщику, 1 - принадлежит застройщику.
        """
        result = 0
        is_builder_raw = bsObj_object.find_all('span', {'data-field': 'isAgency'})
        if len(re.findall(r'Застройщик\n\t', text_block)) > 0:
            result = 1
        elif len(is_builder_raw) > 0:
            is_builder = re.findall('От застройщика', is_builder_raw[0].text)
            if len(is_builder) > 0:
                result = 1
        else:
            result = 0
        return result

    def ad_fields(self, url_list):
        """
        Функция для получения полей объявления.
        Вход: лист с url: 0 - url объвления, 1 - url страницы объявления.
        Выход: data frame с полями таблицы.
        """
        while True:
            try:
                headers = {'User-Agent': UserAgent().chrome}
                proxies = {'https': 'https://' + next(proxy_cycle)}
                session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(max_retries=0)
                session.mount('https://', adapter)
                self.my_session(
                    url=url_list[1],
                    proxies=proxies,
                    headers=headers,
                    session=session,
                    timeout=5
                )
                offers_current = self.my_session(
                    url=url_list[0],
                    proxies=proxies,
                    headers=headers,
                    session=session
                )

                bsObj_object = BeautifulSoup(offers_current.text, 'html5lib')
                # Пишем условие, если блокируют
                if len(re.findall(
                        'Из вашей подсети наблюдается подозрительная активность. Поставьте отметку, чтобы продолжить.',
                        bsObj_object.text)) > 0:
                    continue
                else:
                    title = self.title_ad(bsObj_object)
                    text = self.text_ad(bsObj_object)
                    image = self.image_ad(bsObj_object)
                    price = self.price_ad(bsObj_object)
                    text_block = bsObj_object.find_all('div', {'id': 'fieldsetView'})
                    if len(text_block) > 0:
                        text_block = str(text_block[0].text)
                        address = self.address_ad(text_block)
                        status_house = self.status_house_ad(text_block)
                        area = self.area_ad(text_block)
                        is_mortage = self.is_mortage_ad(text_block)
                        floor = self.floor_ad(text_block)
                    text_clean = self.clean_text(' '.join([str(title), str(text)]))
                    text_lem = self.lem_text(text_clean)
                    id_ad = int(re.findall(r'\d{1,20}.html$', url_list[0])[0].replace('.html', ''))
                    balcony = self.is_balcony(text_clean)
                    builder = self.is_builder_ad(bsObj_object=bsObj_object, text_block=text_block)
                    current_df = pd.DataFrame(
                        {'id': [id_ad], 'title': [title], 'text': [text], 'clean_text': text_clean,
                         'lem_text': text_lem, 'image': [image], 'address': [address],
                         'status_house': [status_house], 'is_builder': builder, 'price': [price], 'area': [area],
                         'is_mortage': [is_mortage], 'floor': [floor],
                         'url': [url_list[0]], 'is_balcony': balcony, 'source': ['farpost']})
                    current_df['load_date'] = [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                break
            except (ConnectTimeout, ProxyError, ConnectionError, ReadTimeout) as e:
                continue
        return current_df



##############################################################################################################################################
############################################# Запускаем код ##################################################################################
##############################################################################################################################################
if __name__ == "__main__":
    farpost_loader = farpost_loader()

    # Получаем прокси сервера
    proxy_df = pd.read_sql(
        con=farpost_loader.engine,
        sql="""
            select 
                    name 
            from 
                    staging_tables.proxy_servers
            where 
                    is_work = True
            """
    )
    # Создадим зацикливавние по прокси серверам
    proxy_cycle = cycle(proxy_df.name)

    # Получим количество страниц и предложений
    offers, pages = farpost_loader.pages_all()

    # Получаем таблицу для обхода
    link_ad_df = farpost_loader.link_ad_all(pages)
    # Перемешаем записи в таблице
    link_ad_df = link_ad_df.sample(frac=1)
    link_ad_df.reset_index(drop=True, inplace=True)
    link_ad_df['result_url'] = link_ad_df.apply(lambda x: [x['url'], x['page']], axis=1)

    # Генерируем табдлицу для обхода
    first_number = 0
    multiple_number = 100
    last_number = link_ad_df.shape[0]

    start_numbers = []
    [start_numbers.append(i) for i in range(first_number, last_number, multiple_number)]
    last_numbers = []
    [last_numbers.append(i) for i in range(multiple_number, last_number + multiple_number, multiple_number)]
    bypass_df = pd.DataFrame({'start_numbers': start_numbers, 'last_numbers': last_numbers})
    # Подменим последнее значение
    bypass_df.loc[bypass_df.shape[0] - 1, 'last_numbers'] = link_ad_df.shape[0] + 1

    # Получаем все объявления
    with ThreadPool(100) as p:
        for i in range(farpost_loader.bypass_df.shape[0]):
            docs = p.map(farpost_loader.ad_fields, farpost_loader.link_ad_df.result_url[farpost_loader.bypass_df.start_numbers[i]:farpost_loader.bypass_df.last_numbers[i]])
            farpost_df = pd.DataFrame()
            current_table = pd.DataFrame()
            for i in docs:
                current_table = pd.concat([current_table, i])
            farpost_df = pd.concat([farpost_df, current_table], sort=False)
            farpost_df.to_sql(
                name='farpost',
                schema='staging_tables',
                con=farpost_loader.engine,
                if_exists='append',
                index=False,
                dtype={
                    'id': sqlalchemy.Integer()
                    , 'title': sqlalchemy.Text()
                    , '"text"': sqlalchemy.Text()
                    , 'clean_text': sqlalchemy.Text()
                    , 'lem_text': sqlalchemy.Text()
                    , 'image': sqlalchemy.JSON()
                    , 'address': sqlalchemy.Text()
                    , 'status_house': sqlalchemy.Boolean()
                    , 'is_builder': sqlalchemy.Boolean()
                    , 'price': sqlalchemy.BigInteger()
                    , 'area': sqlalchemy.FLOAT()
                    , 'is_mortage': sqlalchemy.Boolean()
                    , 'floor': sqlalchemy.Text()
                    , 'url': sqlalchemy.Text()
                    , 'is_balcony': sqlalchemy.Boolean()
                    , 'source': sqlalchemy.Text()
                    , 'load_date': sqlalchemy.DateTime()
                }
            )
