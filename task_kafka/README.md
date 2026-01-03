Порядок запуска:
1) producer_generate_data_pg.py - генерирует данные для postgresql
2) consumer_data_to_pg.py - создает и заполняет таблицу в postgresql
3) producer_pg_to_kafka.py - достает данные из таблицы postgresql
4) consumer_to_clickhouse.py - создает и заполняет таблицу в clickhouse
