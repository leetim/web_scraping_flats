###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с табличными данными
import pandas as pd

# Для работы со временем
import datetime

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
        fields = ['id', 'address', 'built_year', 'floor_count_max', 'floor_count_min', 'entrance_count',
                  'elevators_count', 'living_quarters_count', 'area_residential',
                  'chute_count', 'parking_square', 'wall_material']
        raw_df = pd.read_csv('https://www.reformagkh.ru/opendata/export/87', compression='zip', error_bad_lines=False,
                             sep=';')
        raw_df = raw_df[raw_df.formalname_city == 'Владивосток']
        raw_df = raw_df[fields]
        raw_df.reset_index(drop=True, inplace=True)
        raw_df['load_date'] = update_date
        return [raw_df, description_fields_df]