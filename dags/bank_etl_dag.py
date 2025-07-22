import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date 
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.models.taskinstance import TaskInstance
import pendulum

# --- КОНСТАНТЫ И НАСТРОЙКИ ---
POSTGRES_DS_CONN_ID = 'postgres_bank_ds'
POSTGRES_LOGS_CONN_ID = 'postgres_bank_logs'
DATA_PATH = '/opt/airflow/dags/data/'

FILES_TO_PROCESS = {
    'md_account_d.csv': ('DS', 'MD_ACCOUNT_D', ['account_rk', 'data_actual_date'], ['data_actual_date', 'data_actual_end_date']),
    'md_currency_d.csv': ('DS', 'MD_CURRENCY_D', ['currency_rk', 'data_actual_date'], ['data_actual_date', 'data_actual_end_date']),
    'md_exchange_rate_d.csv': ('DS', 'MD_EXCHANGE_RATE_D', ['data_actual_date', 'currency_rk'], ['data_actual_date', 'data_actual_end_date']),
    'md_ledger_account_s.csv': ('DS', 'MD_LEDGER_ACCOUNT_S', ['ledger_account', 'start_date'], ['start_date', 'end_date']),
    'ft_balance_f.csv': ('DS', 'FT_BALANCE_F', ['on_date', 'account_rk'], ['on_date']),
    'ft_posting_f.csv': ('DS', 'FT_POSTING_F', None, ['oper_date'])
}

def read_csv_smart(filepath):
    """Умная функция чтения CSV с разными кодировками и разделителями"""
    encodings = ['utf-8', 'cp1251', 'koi8-r', 'cp1252']
    separators = [';', ',', '\t']
    
    for enc in encodings:
        for sep in separators:
            try:
                # Сначала читаем первые строки для проверки
                df_test = pd.read_csv(filepath, encoding=enc, sep=sep, decimal=',', nrows=5)
                
                # Проверяем что у нас есть нужные колонки (больше 1 колонки)
                if len(df_test.columns) > 1:
                    # Читаем весь файл
                    df = pd.read_csv(filepath, encoding=enc, sep=sep, decimal=',')
                    logging.info(f"Файл {filepath} успешно прочитан с кодировкой {enc} и разделителем '{sep}'")
                    logging.info(f"Колонки: {list(df.columns)}")
                    logging.info(f"Количество строк: {len(df)}")
                    return df
                    
            except Exception as e:
                logging.debug(f"Не удалось прочитать {filepath} с {enc}/{sep}: {e}")
                continue
                
    # Если ничего не сработало, пробуем прочитать как есть
    try:
        df = pd.read_csv(filepath)
        logging.warning(f"Файл {filepath} прочитан с настройками по умолчанию")
        return df
    except Exception as e:
        raise AirflowFailException(f"Не удалось прочитать файл {filepath}: {e}")

def parse_dates_smart(df, date_columns):
    """Умная функция парсинга дат"""
    for col in date_columns:
        if col in df.columns:
            # Сначала проверим что в колонке
            sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            logging.info(f"Пример значения в колонке {col}: {sample_value}")
            
            # Проверяем количество пустых значений ДО обработки
            null_count_before = df[col].isna().sum()
            logging.info(f"Колонка {col}: {null_count_before} пустых значений до обработки")
            
            # Пробуем разные форматы дат
            original_col = df[col].copy()
            
            # Формат dd.mm.yyyy
            try:
                df[col] = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
                valid_dates = df[col].notna().sum()
                if valid_dates > 0:
                    logging.info(f"Колонка {col}: успешно парсим {valid_dates} дат в формате dd.mm.yyyy")
                    # Заполняем оставшиеся пустые даты значением по умолчанию
                    if df[col].isna().any():
                        default_date = pd.Timestamp('2018-01-01')  # Дата по умолчанию
                        df[col] = df[col].fillna(default_date)
                        logging.warning(f"Колонка {col}: заполнено {df[col].isna().sum()} пустых дат значением {default_date}")
                    continue
            except:
                pass
            
            # Формат yyyy-mm-dd
            try:
                df[col] = pd.to_datetime(original_col, format='%Y-%m-%d', errors='coerce')
                valid_dates = df[col].notna().sum()
                if valid_dates > 0:
                    logging.info(f"Колонка {col}: успешно парсим {valid_dates} дат в формате yyyy-mm-dd")
                    # Заполняем пустые даты
                    if df[col].isna().any():
                        default_date = pd.Timestamp('2018-01-01')
                        df[col] = df[col].fillna(default_date)
                        logging.warning(f"Колонка {col}: заполнено пустых дат значением {default_date}")
                    continue
            except:
                pass
            
            # Автоматический парсинг
            try:
                df[col] = pd.to_datetime(original_col, errors='coerce')
                valid_dates = df[col].notna().sum()
                logging.info(f"Колонка {col}: автопарсинг дал {valid_dates} валидных дат")
                # Заполняем пустые даты
                if df[col].isna().any():
                    default_date = pd.Timestamp('2018-01-01')
                    df[col] = df[col].fillna(default_date)
                    logging.warning(f"Колонка {col}: заполнено пустых дат значением {default_date}")
            except Exception as e:
                logging.error(f"Не удалось распарсить даты в колонке {col}: {e}")
                # В крайнем случае заполняем все значения датой по умолчанию
                df[col] = pd.Timestamp('2018-01-01')
                logging.warning(f"Колонка {col}: все значения заполнены датой по умолчанию 2018-01-01")
            
            # Финальная проверка
            final_null_count = df[col].isna().sum()
            logging.info(f"Колонка {col}: {final_null_count} пустых значений после обработки")

def clean_data_types(df, table_name):
    """Очистка и приведение типов данных для конкретных таблиц"""
    
    def safe_numeric_to_string(series, max_length=10):
        """Безопасное преобразование числовых значений в строки"""
        # Заполняем NaN значения нулями, потом конвертируем в int, потом в string
        return series.fillna(0).astype(int).astype(str).str[:max_length]
    
    if table_name == 'MD_CURRENCY_D':
        # Исправляем currency_code - убираем .0 и приводим к строке
        if 'currency_code' in df.columns:
            df['currency_code'] = safe_numeric_to_string(df['currency_code'], 3)
            
    elif table_name == 'MD_ACCOUNT_D':
        # Исправляем currency_code аналогично
        if 'currency_code' in df.columns:
            df['currency_code'] = safe_numeric_to_string(df['currency_code'], 3)
    
    elif table_name == 'MD_EXCHANGE_RATE_D':
        # Исправляем code_iso_num
        if 'code_iso_num' in df.columns:
            df['code_iso_num'] = safe_numeric_to_string(df['code_iso_num'], 3)
    
    # Обрезаем все строковые поля до разумной длины
    for col in df.columns:
        if df[col].dtype == 'object':  # строковые поля
            if col.endswith('_code') and col not in ['currency_code']:  # уже обработали currency_code выше
                df[col] = df[col].astype(str).str[:10]
            elif 'char_type' in col:
                df[col] = df[col].astype(str).str[:30]
            elif 'name' in col:
                df[col] = df[col].astype(str).str[:255]
    
    return df

@dag(
    dag_id='bank_data_etl_process',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['bank_etl']
)
def bank_etl_dag():
    
    @task()
    def start_log():
        """Логирует начало выполнения всего ETL процесса."""
        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_LOGS_CONN_ID)
            sql = """
                INSERT INTO "LOGS"."ETL_LOGS" (process_name, start_time, status, message)
                VALUES ('Airflow_Full_ETL', NOW(), 'STARTED', 'Начало полной загрузки данных из CSV');
            """
            hook.run(sql, autocommit=True)
            logging.info("Успешно залогировано начало процесса.")
        except Exception as e:
            logging.error(f"Ошибка при логировании начала: {e}")
            raise

    @task()
    def extract_transform_load():
        """Основная задача: извлечение, трансформация и загрузка данных для каждого файла."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_DS_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        for file_name, (schema, table_name, p_keys, date_cols) in FILES_TO_PROCESS.items():
            try:
                full_path = f"{DATA_PATH}{file_name}"
                logging.info(f"Начинаем обработку файла: {full_path}")

                # Читаем CSV с умным парсингом
                df = read_csv_smart(full_path)
                logging.info(f"Прочитано {len(df)} строк из файла {file_name}")

                # Приводим имена колонок к нижнему регистру
                df.columns = [col.lower() for col in df.columns]
                logging.info(f"Колонки после обработки: {list(df.columns)}")

                # Парсим даты с умной функцией
                if date_cols:
                    parse_dates_smart(df, date_cols)
                
                # Очищаем типы данных
                df = clean_data_types(df, table_name)
                
                logging.info(f"Трансформация данных для таблицы {table_name} завершена.")

                # Очищаем таблицу и загружаем данные
                with engine.begin() as connection:
                    connection.execute(text(f'TRUNCATE TABLE "{schema}"."{table_name}" CASCADE'))

                # Удаляем дубликаты в DataFrame перед загрузкой
                if p_keys:
                    # Убираем дубликаты по первичным ключам
                    df_clean = df.drop_duplicates(subset=p_keys, keep='last')
                    logging.info(f"Удалено {len(df) - len(df_clean)} дубликатов по ключам {p_keys}")
                    df = df_clean

                # Загружаем данные
                df.to_sql(
                    table_name,
                    engine,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi'  # Ускоряем загрузку
                )
                logging.info(f"Успешно загружено {len(df)} строк в таблицу {schema}.{table_name}")

            except Exception as e:
                logging.error(f"Ошибка при обработке файла {file_name} для таблицы {table_name}: {e}")
                # Логируем дополнительную информацию для отладки
                if 'df' in locals():
                    logging.error(f"Колонки DataFrame: {list(df.columns)}")
                    logging.error(f"Первые 3 строки:\n{df.head(3)}")
                    if date_cols:
                        for col in date_cols:
                            if col in df.columns:
                                null_count = df[col].isna().sum()
                                logging.error(f"Колонка {col}: {null_count} пустых значений из {len(df)}")
                raise

    @task()
    def finish_log():
        """Логирует успешное завершение всего ETL процесса."""
        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_LOGS_CONN_ID)
            sql = """
                UPDATE "LOGS"."ETL_LOGS"
                SET end_time = NOW(), status = 'SUCCESS', message = 'Полная загрузка данных из CSV успешно завершена'
                WHERE log_id = (SELECT MAX(log_id) FROM "LOGS"."ETL_LOGS" WHERE process_name = 'Airflow_Full_ETL' AND status = 'STARTED');
            """
            hook.run(sql, autocommit=True)
            logging.info("Успешно залогировано завершение процесса.")
        except Exception as e:
            logging.error(f"Ошибка при логировании завершения: {e}")
            raise

    start_task = start_log()
    etl_task = extract_transform_load()
    finish_task = finish_log()

    start_task >> etl_task >> finish_task

bank_etl_dag_instance = bank_etl_dag()