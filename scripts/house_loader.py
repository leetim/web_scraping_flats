###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с табличными данными
import pandas as pd

# Для работы со временем
import datetime

# Для работы с массивами и вычислениями
import numpy as np

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################
class house:
    def __init__(
            self,
            url = 'https://www.reformagkh.ru/opendata?gid=2213474'
    ):
        """
        Функция для инциализации объекта класса.
        Параметры:
        url - текстовое наименование города поиска
        """
        self.url = url

    def house(self):
        """
        Функция для данных  с сайта "Реформа ЖКХ".
        Вход: url набора.
        Выход: таблицы с характеристиками домов и описанием полей.
        """
        # Получаем таблицы настранице
        table = pd.read_html(self.url)
        # Ищем строку с датой изменений
        for i in table:
            current_date = i
            find_string = current_date[(current_date[0] == 'Дата последнего внесения изменений') | (
                        current_date[1] == 'Реестр домов по Приморскому краю')]
            if find_string.shape[0] == 2:
                break
            else:
                continue
        update_date = current_date[current_date[0] == 'Дата последнего внесения изменений'][1].values[0]
        update_date = datetime.datetime.strptime(update_date, "%d.%m.%Y")

        # Ищем строку с описанием полей
        for i in table:
            description_fields_df = i
            find_string = description_fields_df[description_fields_df[1] == 'ID дома на Портале']
            if find_string.shape[0] == 1:
                break
            else:
                continue
        description_fields_df.columns = ['column', 'description']

        # Получаем данные домов
        fields = ['houseguid', 'address', 'built_year', 'floor_count_max', 'floor_count_min', 'entrance_count',
                  'elevators_count', 'living_quarters_count', 'area_residential',
                  'chute_count', 'parking_square', 'wall_material']
        raw_df = pd.read_csv('https://www.reformagkh.ru/opendata/export/87', compression='zip', error_bad_lines=False,
                             sep=';')
        raw_df = raw_df[raw_df.formalname_city == 'Владивосток']
        raw_df = raw_df[fields]
        raw_df['load_date'] = update_date
        raw_df.replace(to_replace='Не заполнено', value=np.nan, inplace=True)
        raw_df['count_info'] = raw_df.count(axis=1)
        # Получаем таблицу с домами-дубликатами
        group_house = raw_df.groupby('houseguid')['address'].count().sort_values(ascending=False).reset_index()
        group_house = group_house[group_house.address > 1]['houseguid'].values.tolist()
        # Получаем дома без домов-дубликатов
        house_df = raw_df[~raw_df.houseguid.isin(group_house)]
        # Получаем уникальные значения для домов-дубликатов
        house_unique = pd.DataFrame()
        for guid in group_house:
            current_df = raw_df[raw_df.houseguid == guid]
            current_max = current_df.count_info.max()
            current_df = current_df[current_df.count_info == current_max]
            if current_df.shape[0] > 1:
                current_df = current_df.head(1)
            house_unique = pd.concat([house_unique, current_df])
        # Соединяем все вместе
        house_df = pd.concat([house_df, house_unique])
        house_df.reset_index(drop=True, inplace=True)
        return [house_df.iloc[:, :13], description_fields_df]