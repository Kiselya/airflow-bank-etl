#!/usr/bin/env python3
"""
Скрипт для экспорта и импорта данных F101 в/из CSV
Задание 1.4: Выгрузка dm.dm_f101_round_f в CSV и загрузка обратно в dm.dm_f101_round_f_v2
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import logging
import os
from datetime import datetime

# Настройки подключения к БД (из контейнера)
DB_CONFIG = {
    'host': 'postgres',      # В Docker сети имя сервиса
    'port': '5432',          # Внутренний порт PostgreSQL
    'database': 'bank_etl',  # ИСПРАВЛЕНО: используем базу bank_etl
    'user': 'ds_user',
    'password': '0510'
}

# Настройки файлов
OUTPUT_DIR = '/opt/airflow/f101_export'
CSV_FILE = 'f101_report.csv'

def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def get_db_connection():
    """Создание подключения к БД"""
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_string)

def create_output_directory():
    """Создание директории для экспорта если её нет"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logging.info(f"Создана директория: {OUTPUT_DIR}")

def export_f101_to_csv():
    """Экспорт данных из dm.dm_f101_round_f в CSV файл"""
    logger = setup_logging()
    
    try:
        logger.info("Начинаем экспорт данных F101 в CSV...")
        
        # Создаем директорию
        create_output_directory()
        
        # Подключение к БД
        engine = get_db_connection()
        
        # SQL запрос для выгрузки всех данных из витрины F101
        sql_query = '''
        SELECT 
            from_date,
            to_date,
            chapter,
            ledger_account,
            characteristic,
            balance_in_rub,
            balance_in_val,
            balance_in_total,
            turn_deb_rub,
            turn_deb_val,
            turn_deb_total,
            turn_cre_rub,
            turn_cre_val,
            turn_cre_total,
            balance_out_rub,
            balance_out_val,
            balance_out_total
        FROM "DM"."DM_F101_ROUND_F"
        ORDER BY ledger_account
        '''
        
        # Выполняем запрос и загружаем данные в DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(sql_query, connection)
        
        logger.info(f"Выгружено {len(df)} строк из витрины F101")
        
        # Путь к файлу
        csv_path = os.path.join(OUTPUT_DIR, CSV_FILE)
        
        # Экспорт в CSV с заголовками колонок
        df.to_csv(
            csv_path, 
            index=False,           # Не включаем индексы
            sep=';',              # Разделитель - точка с запятой
            decimal=',',          # Десятичный разделитель - запятая
            encoding='utf-8',     # Кодировка UTF-8
            date_format='%Y-%m-%d'  # Формат дат
        )
        
        logger.info(f"Данные успешно экспортированы в файл: {csv_path}")
        logger.info(f"Размер файла: {os.path.getsize(csv_path)} байт")
        
        # Показываем первые несколько строк для проверки
        logger.info("Первые 3 строки экспортированных данных:")
        print(df.head(3).to_string())
        
        return csv_path
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте данных: {e}")
        raise

def create_f101_v2_table():
    """Создание копии таблицы dm.dm_f101_round_f_v2"""
    logger = setup_logging()
    
    try:
        logger.info("Создаем таблицу DM.DM_F101_ROUND_F_V2...")
        
        engine = get_db_connection()
        
        # SQL для создания копии таблицы
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS "DM"."DM_F101_ROUND_F_V2" (
            from_date DATE,
            to_date DATE,
            chapter VARCHAR(30),
            ledger_account VARCHAR(5),
            characteristic VARCHAR(30),
            balance_in_rub NUMERIC(23,8),
            balance_in_val NUMERIC(23,8),
            balance_in_total NUMERIC(23,8),
            turn_deb_rub NUMERIC(23,8),
            turn_deb_val NUMERIC(23,8),
            turn_deb_total NUMERIC(23,8),
            turn_cre_rub NUMERIC(23,8),
            turn_cre_val NUMERIC(23,8),
            turn_cre_total NUMERIC(23,8),
            balance_out_rub NUMERIC(23,8),
            balance_out_val NUMERIC(23,8),
            balance_out_total NUMERIC(23,8)
        );
        '''
        
        with engine.begin() as connection:  # begin() вместо connect()
            connection.execute(text(create_table_sql))
            # commit() убираем - begin() делает автокоммит
        
        logger.info("Таблица DM_F101_ROUND_F_V2 успешно создана")
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise

def import_csv_to_f101_v2(csv_file_path=None):
    """Импорт данных из CSV в dm.dm_f101_round_f_v2"""
    logger = setup_logging()
    
    try:
        # Если путь к файлу не указан, используем стандартный
        if csv_file_path is None:
            csv_file_path = os.path.join(OUTPUT_DIR, CSV_FILE)
        
        logger.info(f"Начинаем импорт данных из CSV: {csv_file_path}")
        
        # Проверяем существование файла
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"Файл не найден: {csv_file_path}")
        
        # Читаем CSV файл
        df = pd.read_csv(
            csv_file_path,
            sep=';',
            decimal=',',
            encoding='utf-8',
            parse_dates=['from_date', 'to_date']  # Автоматически парсим даты
        )
        
        logger.info(f"Прочитано {len(df)} строк из CSV файла")
        
        # Создаем таблицу v2 если её нет
        create_f101_v2_table()
        
        # Подключение к БД
        engine = get_db_connection()
        
        # Очищаем таблицу перед загрузкой
        with engine.begin() as connection:  # begin() вместо connect()
            connection.execute(text('TRUNCATE TABLE "DM"."DM_F101_ROUND_F_V2"'))
        
        logger.info("Таблица DM_F101_ROUND_F_V2 очищена")
        
        # Загружаем данные в таблицу
        df.to_sql(
            'DM_F101_ROUND_F_V2',
            engine,
            schema='DM',
            if_exists='append',
            index=False,
            method='multi'  # Ускоряем загрузку
        )
        
        logger.info(f"Успешно импортировано {len(df)} строк в таблицу DM_F101_ROUND_F_V2")
        
        # Проверяем результат
        with engine.begin() as connection:  # begin() вместо connect()
            result = connection.execute(text('SELECT COUNT(*) FROM "DM"."DM_F101_ROUND_F_V2"'))
            count = result.fetchone()[0]
            logger.info(f"Проверка: в таблице DM_F101_ROUND_F_V2 находится {count} записей")
        
    except Exception as e:
        logger.error(f"Ошибка при импорте данных: {e}")
        raise

def modify_csv_example(csv_file_path=None):
    """Пример изменения нескольких значений в CSV файле"""
    logger = setup_logging()
    
    try:
        if csv_file_path is None:
            csv_file_path = os.path.join(OUTPUT_DIR, CSV_FILE)
        
        logger.info(f"Изменяем данные в файле: {csv_file_path}")
        
        # Читаем CSV
        df = pd.read_csv(csv_file_path, sep=';', decimal=',', encoding='utf-8')
        
        # Показываем оригинальные данные первых строк
        logger.info("Оригинальные данные (первые 2 строки):")
        print(df.head(2)[['ledger_account', 'balance_out_rub', 'balance_out_val']].to_string())
        
        # Изменяем несколько значений для демонстрации
        if len(df) > 0:
            # Изменяем остатки в первой строке
            df.loc[0, 'balance_out_rub'] = df.loc[0, 'balance_out_rub'] * 1.1  # Увеличиваем на 10%
            df.loc[0, 'balance_out_val'] = df.loc[0, 'balance_out_val'] * 0.9  # Уменьшаем на 10%
            
        if len(df) > 1:
            # Изменяем обороты во второй строке
            df.loc[1, 'turn_deb_rub'] = df.loc[1, 'turn_deb_rub'] * 1.05  # Увеличиваем на 5%
            df.loc[1, 'turn_cre_rub'] = df.loc[1, 'turn_cre_rub'] * 0.95  # Уменьшаем на 5%
        
        # Сохраняем измененный файл
        modified_file = csv_file_path.replace('.csv', '_modified.csv')
        df.to_csv(
            modified_file,
            index=False,
            sep=';',
            decimal=',',
            encoding='utf-8',
            date_format='%Y-%m-%d'
        )
        
        logger.info(f"Измененные данные сохранены в: {modified_file}")
        logger.info("Измененные данные (первые 2 строки):")
        print(df.head(2)[['ledger_account', 'balance_out_rub', 'balance_out_val']].to_string())
        
        return modified_file
        
    except Exception as e:
        logger.error(f"Ошибка при изменении CSV: {e}")
        raise

def main():
    """Основная функция - выполняет полный цикл экспорт -> изменение -> импорт"""
    logger = setup_logging()
    
    try:
        logger.info("=== НАЧАЛО ВЫПОЛНЕНИЯ ЗАДАНИЯ 1.4 ===")
        
        # 1. Экспортируем данные F101 в CSV
        csv_file = export_f101_to_csv()
        
        # 2. Изменяем несколько значений в CSV
        modified_csv = modify_csv_example(csv_file)
        
        # 3. Импортируем измененные данные в новую таблицу
        import_csv_to_f101_v2(modified_csv)
        
        logger.info("=== ЗАДАНИЕ 1.4 УСПЕШНО ВЫПОЛНЕНО ===")
        logger.info(f"Экспортированный файл: {csv_file}")
        logger.info(f"Измененный файл: {modified_csv}")
        logger.info("Данные загружены в таблицу DM.DM_F101_ROUND_F_V2")
        
    except Exception as e:
        logger.error(f"Ошибка выполнения задания: {e}")
        raise

if __name__ == "__main__":
    main()