#!/usr/bin/env python3
"""
Упрощенный скрипт для импорта CSV в таблицу DM_F101_ROUND_F_V2
Для запуска через Docker: docker-compose exec script-runner python3 /opt/airflow/py_scripts/import_csv_to_f101_v2.py
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import logging
import os

# Настройки из переменных окружения или значения по умолчанию
DB_HOST = os.getenv('PG_HOST', 'postgres')  # В Docker сети
DB_PORT = os.getenv('PG_PORT', '5432')      # Внутренний порт
DB_NAME = os.getenv('PG_DATABASE', 'bank_etl')  # ИСПРАВЛЕНО: база bank_etl
DB_USER = os.getenv('PG_USER', 'ds_user')
DB_PASSWORD = os.getenv('PG_PASSWORD', '0510')

# Путь к CSV файлу
CSV_FILE_PATH = '/opt/airflow/f101_report.csv'

def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def main():
    """Основная функция импорта CSV в таблицу DM_F101_ROUND_F_V2"""
    logger = setup_logging()
    
    try:
        logger.info(f"Начинаем импорт из {CSV_FILE_PATH} в DM.DM_F101_ROUND_F_V2...")
        
        # Проверяем наличие файла
        if not os.path.exists(CSV_FILE_PATH):
            logger.error(f"Файл не найден: {CSV_FILE_PATH}")
            return False
        
        # Создаем подключение к БД
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        
        logger.info("Подключение к БД установлено")
        
        # Читаем CSV файл
        try:
            df = pd.read_csv(
                CSV_FILE_PATH,
                sep=';',
                decimal=',',
                encoding='utf-8'
            )
            logger.info(f"Прочитано {len(df)} строк из CSV файла")
        except Exception as e:
            logger.error(f"Ошибка чтения CSV файла: {e}")
            return False
        
        # Преобразуем колонки дат
        date_columns = ['from_date', 'to_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Очищаем целевую таблицу
        with engine.connect() as connection:
            connection.execute(text('TRUNCATE TABLE "DM"."DM_F101_ROUND_F_V2"'))
            connection.commit()
            logger.info("Таблица DM_F101_ROUND_F_V2 очищена")
        
        # Загружаем данные
        df.to_sql(
            'DM_F101_ROUND_F_V2',
            engine,
            schema='DM',
            if_exists='append',
            index=False
        )
        
        logger.info(f"Успешно импортировано {len(df)} строк в DM_F101_ROUND_F_V2")
        
        # Проверяем результат
        with engine.connect() as connection:
            result = connection.execute(text('SELECT COUNT(*) FROM "DM"."DM_F101_ROUND_F_V2"'))
            count = result.fetchone()[0]
            logger.info(f"Проверка: в таблице {count} записей")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при импорте: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)