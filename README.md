# Анализ рынка недвижимости (г. Владивосток. )
## Установка необходимых компонентов:
**Установим необходимые пакеты:**<br>
<code>sudo apt install postgis<code><br>
**Установим расширение в pg:**<br>
<code>sudo -u postgres psql<code><br>
**Создаем расширение в pg**<br>
<code>create extension postgis;<code><br>
  
## Схема данных: <br>
![img](https://github.com/VolokzhaninVadim/web_scraping_flats/blob/master/ddl_dml/data_scheme.png)
## Код для crontab -e
<code># Запуск обработчика объявлений farpost<code><br>
<code>0 23 * * * /mnt/sda1/Documents/Projects/web_scraping_flats/scripts/farpost_scheduler.sh > /mnt/sda1/Documents/Projects/web_scraping_flats/scripts/farpost_log.txt 2>&1<\code>


