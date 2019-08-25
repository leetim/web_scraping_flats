# Обработка HTML
import requests

# Для работы с табличными данными
import pandas as pd

# Для работы с регулярными выражениями
import re

# Для работы с массивами и вычислениями
import numpy as np

# Для работы с SQL
from sqlalchemy import create_engine
import sqlalchemy

# Загрузка proxy server
from proxyscrape import create_collector

# Для работы с операционной системой
import os

# Для работа со временем
import datetime

# Для паралельной работы кода
from multiprocessing.dummy import Pool as ThreadPool

# Инициализируем строку подключения
engine = create_engine('postgres://volokzhanin:{password}@localhost:5432/volokzhanin'.format(password = os.getenv('PASSWORD1', False)))

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################
class proxy_loader:
    def __init__(
            self,
            url = 'https://www.farpost.ru/vladivostok/',
            n_proxy = 1000,
            type_proxies = 'https',
            table = 'proxy_servers',
            schema = 'staging_tables'
    ):
        """
        Функция для инциализации объекта класса.
        Параметры:
        url - url для проверки proxy,
        n_proxy - количество proxy,
        table - наименование таблицы для записи,
        schema - наименование схемы для записи.
        """
        self.url = url
        self.n_proxy = n_proxy
        self.type_proxies = type_proxies
        self.table = table
        self.schema = schema

    def get_proxy(self, collector):
        """
        Функция для получения прокси-сервера
        Параметры: https anonymous proxies
        Выход: proxy *.*.*.*:*
        """
        proxy = collector.get_proxy()
        return '{}:{}'.format(proxy.host, proxy.port)

    def get_proxy_servers(self):
        """
        Функция для получения списка прокси.
        Параметры:
        n_proxy: количество прокси-серверов, type_proxies: тип прокси-сервера (https anonymous proxies).
        Выход: таблица прокси-серверов.
        """
        collector = create_collector('collector {type_proxies}'.format(type_proxies = self.type_proxies), self.type_proxies)
        collector.apply_filter({'anonymous': True})
        proxy_df = pd.DataFrame({'name': [self.get_proxy(collector) for proxy in range(self.n_proxy)]})
        proxy_df['load_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        proxy_df.reset_index(inplace = True)
        proxy_df.rename(columns = {'index': 'id'}, inplace =  True)
        return proxy_df

    def get_check_proxy(self, proxy):
        """
        Функция для проверки прокси с 'http://spys.one/proxys/RU/'.
        Параметры: прокси.
        Выход: 0 - доступен, 1 - не доступен.
        """
        url = self.url
        try:
            page = requests.get(
                url,
                proxies = {'https': 'https://' + proxy},
                timeout = 10
            )
            if page.status_code == 200:
                status = 1
            else:
                status = 0
        except:
            status = 0
        return status

    def write_check_proxy(self):
        """
        Функция для записи и получения проверенных proxy servers.
        Параметры: нет.
        Выход: запись proxy в бд и таблица проверенных proxy
        """

        # Получаем proxy
        df = self.get_proxy_servers()

        # Проверяем proxy
        pool = ThreadPool(10)
        results_proxy_list = pool.map(self.get_check_proxy, df.name)
        df['is_work'] = results_proxy_list

        # Записываем данные
        df.to_sql(
            name = self.table,
            schema = self.schema,
            con = engine,
            if_exists = 'replace',
            index  =  False,
            dtype  = {
                "id": sqlalchemy.Integer()
                , 'load_date': sqlalchemy.DateTime()
                , 'name': sqlalchemy.Text()
                , 'is_work': sqlalchemy.Boolean()
            }
        )
        return df

##############################################################################################################################################
############################################# Запускаем код ##################################################################################
##############################################################################################################################################
if __name__ == "__main__":
    proxy_loader = proxy_loader()
    proxy_loader.write_check_proxy()
