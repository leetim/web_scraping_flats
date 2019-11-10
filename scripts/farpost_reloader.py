# -*- coding: utf-8 -*-
###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с HTTP-запросами 
import requests
from requests import ConnectTimeout, ConnectionError, ReadTimeout 
from requests.exceptions import ProxyError
import urllib
# Для генерации поддельного User agent
from fake_useragent import UserAgent
ua = UserAgent()

# Для работы с операционной системой
import os

# Для работы с табличными данными
import pandas as pd

# Для работы с JSON
import json

# Для работы с геопространственными данными
from shapely.geometry import Point, Polygon

# Для работы с регулярными выражениями 
import re

# Для работы с SQL
import sqlalchemy
from sqlalchemy import create_engine
# Для работы с Postgre
import psycopg2

# Для параллельной работы кода
from multiprocessing.dummy import Pool as ThreadPool 

# Для произведения синтаксического анализа (лемматизации)
import pymorphy2 as pm
# Загрузим словарь русского языка
morph = pm.MorphAnalyzer()

# Для работы со временем
import datetime

# Импортируем класс tor 
os.chdir('/mnt/sda1/Documents/Projects/web_scraping_flats/scripts')
from tor_crawler import TorCrawler
crawler = TorCrawler(ctrl_pass='1234') 

# Создадим подключние к dwh
engine = create_engine('postgres://volokzhanin:{password}@localhost:5432/volokzhanin'.format(password = os.getenv('PASSWORD1', False)))

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################
class farpost_reloader:
    def __init__(
            self
    ):
        """
        Функция для инциализации объекта класса.
        Параметры:
        url - нет
        """

    def clean_text(self, text) -> str:
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

    def lem_text(self, text) -> str:
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
        return(finish_text)

    def clean_ad(self, text): 
        """
        Функция для очистки текста объявления. 
        Вход: сырой текст. 
        Выход: очищенный тескст. 
        """
        tamplate = re.compile('\n|\t| во Владивостоке|Подробности о доме|Адрес|Этаж')
        clean_text = ' '.join(tamplate.sub(' ', text).split()).strip()
        return clean_text

    def address_ad(self, text_block) -> str: 
        """
        Функция для получения адреса объявления. 
        Вход: текст для извлечения адреса.
        Выход: адрес объявления.  
        """
        raw_address = re.findall('Адрес[\t\n\r]+.+', text_block)
        if len(raw_address) > 0: 
            address = 'Россия, Приморский край, Владивосток, ' + self.clean_ad(raw_address[0])
        else: 
            address = None
        return address

    def title_ad(self, bsObj) -> str: 
        """
        Функция для получения заголовка объявления. 
        Вход: beautiful soup объект.
        Выход: заголовок объявления.  
        """
        title = bsObj.find_all('h1', {'class' : 'subject viewbull-field__container'})
        if len(title) > 0: 
            title = self.clean_ad(title[0].text)
        else: 
            title = ''
        return title

    def image_ad(self, bsObj) -> list: 
        """
        Функция для получения изображений объявления. 
        Вход: beautiful soup объект.
        Выход: лист изображений объявления.  
        """
        image = bsObj.find_all('img')
        if len(image) > 0:
            image_list = []
            for im in image: 
                current_image = re.findall(r'v/\d{1,100}_bulletin', str(im))
                if len(current_image) > 0: 
                    image_list.append('https://static.baza.farpost.ru/' + current_image[0])
        else: 
            image_list = None
        return image_list

    def price_ad(self, bsObj) -> int: 
        """
        Функция для получения цены в объявлении. 
        Вход: beautiful soup объект.
        Выход: цена в объявлении.  
        """
        price = bsObj.find_all('span', {'class' : 'viewbull-summary-price__value'})
        if len(price) > 0: 
            price = price[0].text
            price = re.sub(r'≈|\s', '', price)
            price = re.findall(r'\d{1,}₽', price)[0]
            price = int(re.sub('₽', '', price))       
        else: 
            price = None
        return price

    def status_house_ad(self, text_block) -> int: 
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

    def area_ad(self, text_block) -> int: 
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

    def is_mortage_ad(self, text_block) -> int: 
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

    def floor_ad(self, text_block) -> str: 
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

    def text_ad(self, bsObj) -> str:
        """
        Функция для получения текста объявления.
        Вход: текст для извлечения текста объявления.
        Выход: текста объявления.
        """
        text = self.clean_ad(bsObj.text)
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

    def is_balcony(self, text) -> int:
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

    def is_builder_ad(self, bsObj, text_block) -> int:
        """
        Функция для возвращения принадлежности объявления к застройщику.
        Вход: bsObj_object - beautiful soup объект, text_block - текстовый блок.
        Выход: 0 - не принадлежит застройщику, 1 - принадлежит застройщику.
        """
        result = 0
        is_builder_raw = bsObj.find_all('span', {'data-field' : 'isAgency'})    
        if len(re.findall(r'Застройщик\n\t' , text_block)) > 0:
            result = 1
        elif len(is_builder_raw) > 0:
            is_builder = re.findall('От застройщика', is_builder_raw[0].text)
            if len(is_builder) > 0:
                result = 1
        else: 
            result = 0
        return result

    def city_polygon(self, city = 'Владивосток') -> dict:
        """
        Функция для возвращения полигона города. 
        Вход: наименование города.
        Выход: gojson. 
        """
        url = 'https://nominatim.openstreetmap.org/search?'
        params = {'format' : 'json', 'limit' : '1', 'polygon_geojson' : '10', 'city' : city}
        city = requests.get(url, params = params)
        return city.json()[0]

    def url_id_ad(self, url) -> int: 
        '''
        Функция для получения id фарпост.
        Вход: url объявления.
        Вход: id объявления. 
        '''
        raw_url = re.findall(r'\d{1,30}.html', url)
        url = int(re.sub(r'\D', '', raw_url[0]))
        return url

    def link_add(self) -> pd.DataFrame():
        """
        Функция для получения ссылок на объявления farpost.
        Вход: нет.
        Выход: таблица с сылками на объявления.
        """
        url = 'https://www.farpost.ru/map/35?city=1115&&leftBottom=42.696899196264845,131.66135178432813&rightTop=43.62049420086799,132.30879856167186&chunk=1&chunkSize=100000'
        bsObj = crawler.get(
            url = url, 
            headers={'User-Agent': UserAgent().chrome}
        )
        points_raw = json.loads(str(bsObj))

        # Получаем полигон Владивостока
        vladivostok_polygon_osm = self.city_polygon()
        vladivostok_polygon = Polygon(vladivostok_polygon_osm['geojson']['coordinates'][0])

        link_ad_df = pd.DataFrame()
        for point in points_raw['points']: 
            longitude = point['lng']
            latitude = point['lat']
            address = point['addr']
            current_df = pd.DataFrame()
            for ad in point['bs']:
                price = int(re.sub(r'\D', '', ad['price']).strip()) if 'price' in ad else None
                tittle = ad['subject']
                url = 'https://www.farpost.ru' + ad['url']
                img = 'https://static.baza.farpost.ru/v/' + str(ad['img']) if 'img' in ad else None
                current_df = pd.concat([current_df, pd.DataFrame({
                    'url' : [url], 
                    'img' : [img], 
                    'tittle' : [tittle], 
                    'price' : [price]
                })], sort = False)
            current_df['longitude'] = longitude
            current_df['latitude'] = latitude
            current_df['address'] = address
            link_ad_df = pd.concat([link_ad_df, current_df], sort = False)    
        link_ad_df['id'] = link_ad_df.url.apply(lambda x: self.url_id_ad(x)) 
        # Удаляем объявления 
        link_ad_df = link_ad_df[(~link_ad_df.price.isna()) & (~link_ad_df.img.isna())]
        # Удаляем адреса, которые не из Владивостока
        link_ad_df['point'] = link_ad_df.apply(lambda x: Point(x['longitude'], x['latitude']), axis = 1)
        link_ad_df['is_intersect'] = link_ad_df.apply(lambda x: vladivostok_polygon.intersects(x['point']), axis = 1) 
        link_ad_df = link_ad_df[link_ad_df.is_intersect == True]
        link_ad_df = link_ad_df.iloc[:, :8].copy()
        link_ad_df['address'] = link_ad_df.address.apply(lambda x: 'Россия Приморский край Владивосток, ' + x)
        link_ad_df.reset_index(drop = True, inplace = True)
        return link_ad_df

    def ad_fields(self, url) -> pd.DataFrame(): 
        """
        Функция для получения полей объявления. 
        Вход: страница объявления. 
        Выход: data frame с полями таблицы. 
        """
        while True: 
            crawler.rotate()
            bsObj = crawler.get(url, headers={'User-Agent': UserAgent().chrome})
            id_ad = self.url_id_ad(url)
            # Если нас proxy заблокировали, то меняем proxy
            if len(re.findall('Из вашей подсети наблюдается подозрительная активность. Поставьте отметку, чтобы продолжить.', bsObj.text)) > 0:
                continue
            else:
                title = self.title_ad(bsObj)
                text = self.text_ad(bsObj)
                image = self.image_ad(bsObj)
                price = self.price_ad(bsObj)
                text_block = bsObj.find_all('div',{'id' : 'fieldsetView'})
                if len(text_block) > 0: 
                    text_block =  text_block[0].text
                    address = self.address_ad(text_block)
                    status_house = self.status_house_ad(text_block)
                    area = self.area_ad(text_block)
                    is_mortage = self.is_mortage_ad(text_block)
                    floor = self.floor_ad(text_block) 
                    text_clean = self.clean_text(' '.join([str(title), str(text)]))
                    text_lem = self.lem_text(text_clean)                 
                    balcony = self.is_balcony(text_clean)
                    builder = self.is_builder_ad(bsObj = bsObj, text_block = text_block)
                    current_df = pd.DataFrame({'id' : [id_ad], 'title' : [title], 'text' : [text], 'clean_text' : [text_clean], 'lem_text' : [text_lem], 'image' : [image], 'address' : [address], 
                                            'status_house' : [status_house], 'is_builder' : [builder], 'price' : [price], 'area' : [area], 'is_mortage' : [is_mortage], 'floor' : [floor], 
                                            'url' : [url], 'is_balcony' : balcony})              
                    
                else: 
                    current_df = pd.DataFrame({'id' : [id_ad], 'title' : [None], 'text' : [None], 'clean_text' : [None], 'lem_text' : [None], 'image' : [None], 'address' : [None], 
                                            'status_house' : [None], 'is_builder' : [None], 'price' : [None], 'area' : [None], 'is_mortage' : [None], 'floor' : [None], 
                                            'url' : [None], 'is_balcony' : [None]}) 
                current_df['load_date'] = [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")] 
                break
        return current_df

###############################################################################################################################################
############################################## Запускаем код ##################################################################################
###############################################################################################################################################
if __name__ == "__main__":
    farpost_reloader = farpost_reloader()

    # Получим ссылки и запишем их
    link_ad_df = farpost_reloader.link_add()
    link_ad_df.to_sql(
                name = 'farpost_link',
                schema = 'staging_tables',
                con = engine,
                if_exists = 'replace',
                index = False
    )

    # Получаем новые ссылки для обхода
    query = """
    select 
            link.*
    from 
            staging_tables.farpost_link  as link 
    left join farpost.farpost as farpost on link.id = farpost.id
    where 
            farpost.id is null
    """
    new_link_ad_df = pd.read_sql(
        con = engine,
        sql = query
    )

    # Обходим все новые ссылки и записываем их
    with ThreadPool(200) as p:
        docs = p.map(farpost_reloader.ad_fields, new_link_ad_df.url)
        current_table = pd.DataFrame()
        for i in docs: 
            current_table = pd.concat([current_table, i])
        # Оставляем заполненные объявления
        current_table = current_table[~current_table.title.isna()]
        # Получим дополнительные поля
        result_df = current_table.merge(right = new_link_ad_df[['id', 'img', 'longitude', 'latitude']], how = 'inner', on = 'id')
        result_df.rename(columns = {'img' : 'img_main'}, inplace = True)        
        result_df.to_sql(
            name = 'farpost',
            schema = 'farpost',
            con = engine,
            if_exists = 'append',
            index = False,
            dtype = {
                'id' : sqlalchemy.Integer()
                , 'title': sqlalchemy.Text()
                , '"text"': sqlalchemy.Text()
                , 'clean_text': sqlalchemy.Text()
                , 'lem_text' : sqlalchemy.Text()
                , 'image' : sqlalchemy.JSON()
                , 'address': sqlalchemy.Text()
                , 'status_house' : sqlalchemy.Boolean()
                , 'is_builder' : sqlalchemy.Boolean()
                , 'price' : sqlalchemy.BigInteger() 
                , 'area' : sqlalchemy.FLOAT() 
                , 'is_mortage' : sqlalchemy.Boolean()
                , 'floor' : sqlalchemy.Text()
                , 'url' : sqlalchemy.Text()
                , 'is_balcony' : sqlalchemy.Boolean()
                , 'source' : sqlalchemy.Text()
                , 'load_date' : sqlalchemy.DateTime()
                , 'img' : sqlalchemy.Text()
                , 'longitude' : sqlalchemy.Float()
                , 'latitude' : sqlalchemy.Float()
            }
        )

    