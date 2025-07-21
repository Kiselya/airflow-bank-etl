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

# --- УМНАЯ ФУНКЦИЯ ЧТЕНИЯ CSV ---
def read_csv_smart(filepath):
    encodings = ['utf-8', 'cp1251', 'koi8-r']
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, sep=';', decimal=',')
            logging.info(f"Файл {filepath} успешно прочитан с кодировкой {enc}")
            return df
        except UnicodeDecodeError:
            logging.warning(f"Не удалось прочитать {filepath} с кодировкой {enc}, пробую следующую...")
    raise AirflowFailException(f"Не удалось декодировать файл {filepath} ни одной из кодировок.")

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

                df = read_csv_smart(full_path)
                logging.info(f"Прочитано {len(df)} строк из файла {file_name}")

                df.columns = [col.lower() for col in df.columns]

                for col in date_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                
                logging.info(f"Трансформация данных для таблицы {table_name} завершена.")

                with engine.connect() as connection:
                    connection.execute(text(f'TRUNCATE TABLE "{schema}"."{table_name}"'))
                    connection.commit()

                df.to_sql(
                    table_name,
                    engine,
                    schema=schema,
                    if_exists='append',
                    index=False
                )
                logging.info(f"Успешно загружено {len(df)} строк в таблицу {schema}.{table_name}")

            except Exception as e:
                logging.error(f"Ошибка при обработке файла {file_name} для таблицы {table_name}: {e}")
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
