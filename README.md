# Анализ рынка недвижимости (г. Владивосток. )
## Установка необходимых компонентов:
**Установим необходимые пакеты:**<br>
```sudo apt install postgis на сервер.```<br>
**Установим расширение в pg:**<br>
```sudo -u postgres psql```<br>
**Создаем расширение в pg**<br>
```create extension postgis;```<br>

## Схема данных: <br>
![img](https://github.com/VolokzhaninVadim/web_scraping_flats/blob/master/ddl_dml/data_scheme.png)
## Код для crontab -e
<code># Запуск обработчика объявлений farpost<code>
0 23 * * * /mnt/sda1/Documents/Projects/web_scraping_flats/scripts/farpost_scheduler.sh > /mnt/sda1/Documents/Projects/web_scraping_flats/scripts/farpost_log.txt 2>&1


