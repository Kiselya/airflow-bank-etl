import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date 
import time
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.models.taskinstance import TaskInstance

# Добавляем константы и настройки
POSTGRES_DS_CONN_ID = 'postgres_bank_ds'
POSTGRES_LOGS_CONN_ID = 'postgres_bank_logs'
DATA_PATH = '/opt/airflow/dags/data/'

FILES_TO_PROCESS = {
    'md_account_d.csv': ('DS', 'MD_ACCOUNT_D', ['account_rk', 'data_actual_date'], ['data_actual_date', 'data_actual_end_date'], 'cp1251'),
    'md_currency_d.csv': ('DS', 'MD_CURRENCY_D', ['currency_rk', 'data_actual_date'], ['data_actual_date', 'data_actual_end_date'], 'koi8-r'),
    'md_exchange_rate_d.csv': ('DS', 'MD_EXCHANGE_RATE_D', ['data_actual_date', 'currency_rk'], ['data_actual_date', 'data_actual_end_date'], 'cp1251'),
    'md_ledger_account_s.csv': ('DS', 'MD_LEDGER_ACCOUNT_S', ['ledger_account', 'start_date'], ['start_date', 'end_date'], 'koi8-r'),
    'ft_balance_f.csv': ('DS', 'FT_BALANCE_F', ['on_date', 'account_rk'], ['on_date'], 'cp1251'),
    'ft_posting_f.csv': ('DS', 'FT_POSTING_F', None, ['oper_date'], 'cp1251')
}

@dag(
    # Задаем имя нашего DAG-а, описание, параметр даты создания, выбираем мануальный запуск, добавляем тэги
    dag_id='bank_data_etl_process',
    description='ETL process for loading bank data from CSV to PostgreSQL',
    start_date=datetime(2025, 7, 10),
    schedule=None,
    catchup=False,
    tags=['bank_project', 'etl'],
)
def bank_etl_dag():
    @task()
    def start_log():
        try:
            # Хуком устанавливаем подключение, используя подключение postgres_bank_logs
            hook = PostgresHook(postgres_conn_id=POSTGRES_LOGS_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            # Открываем транзакцию к базе данных
            with engine.connect() as conn:
                # Выполяем SQL-запрос INSERT, который добавляет новую строку в таблицу "LOGS"."ETL_LOGS"
                # Он записывает имя процесса, текущее время и выставляет статус STARTED
                stmt = text(
                'INSERT INTO "LOGS"."ETL_LOGS" (process_name, start_time, status) VALUES (\'Airflow_Full_ETL\', :start_time, \'STARTED\') RETURNING log_id;')
                result = conn.execute(stmt, {'start_time': datetime.now()})
                log_id = result.scalar()
                logging.info(f"Process 'Airflow_Full_ETL' started. Log ID: {log_id}")
                # log_id используем для отслеживания запусков
                return log_id
        except Exception as e:
            raise AirflowFailException(f"Failed to log start: {e}")

    @task()
    def extract_transform_load(ti: TaskInstance):
        log_id = ti.xcom_pull(task_ids='start_log', key='return_value')
        total_rows = 0
        # Устанавливает второе, отдельное соединение с базой данных,
        # но на этот раз используя postgres_bank_ds для доступа к схеме с данными DS
        ds_hook = PostgresHook(postgres_conn_id=POSTGRES_DS_CONN_ID)
        engine_ds = ds_hook.get_sqlalchemy_engine()
        
        try:
            # Начинается цикл, который будет по очереди обрабатывать каждый файл, указанный словаре FILES_TO_PROCESS
            for filename, (schema, table, p_keys, date_cols, encoding) in FILES_TO_PROCESS.items():
                filepath = f"{DATA_PATH}{filename}"
                logging.info(f"===== Processing file {filepath} for table {table} with encoding {encoding} =====")
                # Первый шаг ETL — извлечение, разделить ';', файлы разделены запятой, используем правильные кодировки (спасибо, я отлично их поИСКАЛ)
                df = pd.read_csv(filepath, sep=';', decimal=',', encoding=encoding)
                df.columns = df.columns.str.lower()
                
                # Проводим очистку данных и преобразование типов
                for col in date_cols:
                    if col in df.columns:
                        # 'coerce' используется как "защита от дурака"
                        # Если в колонке с датой попадется мусор (пустая строка),
                        # код не упадет с ошибкой, а просто поставит в этом месте специальную метку 'NaT'
                        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                # Если в колонках дат появилась метка 'NaT', то полностью удаляет эти строки из данных.
                df.dropna(subset=date_cols, inplace=True)
                # Проводим явное преобразование 'reduced_cource' в числовое поле значений
                if 'reduced_cource' in df.columns:
                    df['reduced_cource'] = pd.to_numeric(df['reduced_cource'], errors='coerce')
                
                # Проводим явное преобразование 'balance_out' в числовое поле значений
                if 'balance_out' in df.columns:
                    df['balance_out'] = pd.to_numeric(df['balance_out'], errors='coerce')
                # NaT, которое появляется, когда дату не удается распознать, некорректно преобразуется в NULL базы данных.
                # Эта строка явно заменяет все значения NaT на None из Python, который SQLAlchemy уже правильно переводит
                # в NULL для базы данных, предотвращая ошибки
                df = df.replace({pd.NaT: None})

                column_dtypes = {col: Date for col in date_cols if col in df.columns}
                # Очищаем таблицу полностью перед получением новой порции данных
                if table == 'FT_POSTING_F':
                    with engine_ds.connect() as conn:
                        conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}";'))
                    df.to_sql(table, engine_ds, schema=schema, if_exists='append', index=False, method='multi', dtype=column_dtypes)
                # Здесь логика следующая: Сначала загружаем данные из CSV в новую временную таблицу в базе данных
                # Затем выполняется специальная SQL-команда INSERT...., где мы вставляем данные из временной в основную
                # Если строка с таким же первичным ключом уже существует, не упадет с ошибкой,
                # А вместо этого обновляем существующую строку новыми значениями.
                # После успешного слияния данных временная таблица удаляется.
                else:
                    temp_table_name = f"temp_{table.lower()}"
                    full_table_name = f'"{schema}"."{table}"'
                    df.to_sql(temp_table_name, engine_ds, schema=schema, if_exists='replace', index=False,
                                method='multi', dtype=column_dtypes) 
                    cols = ", ".join([f'"{c}"' for c in df.columns])
                    pk_cols = ", ".join([f'"{c}"' for c in p_keys])
                    update_cols = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c not in p_keys])
                    sql_upsert = f"INSERT INTO {full_table_name} ({cols}) SELECT {cols} FROM \"{schema}\".\"{temp_table_name}\" ON CONFLICT ({pk_cols}) DO UPDATE SET {update_cols};"
                    with engine_ds.begin() as conn:
                        conn.execute(text(sql_upsert))
                        conn.execute(text(f'DROP TABLE "{schema}"."{temp_table_name}";'))
                # Подсчитывает общее количество обработанных строк по всем файлам,
                # если будет ошибка в цикле(чтение, преобразование, загрузка в БД),
                # то весь блок прерывается, а ошибка логгируется, ETL предотвращает кривую загрузку данных
                total_rows += len(df)
            return total_rows
        except Exception as e:
            logging.error(f"ERROR IN ETL TASK: {e}")
            raise AirflowFailException(f"ETL process failed with error: {e}")

    @task()
    def finish_log(ti: TaskInstance):
        # ti.xcom_pull забирает log_id из задачи start_log.
        # Она забирает общее количество обработанных строк (rows_processed) из задачи extract_transform_load.
        log_id = ti.xcom_pull(task_ids='start_log', key='return_value')
        rows_processed = ti.xcom_pull(task_ids='extract_transform_load', key='return_value')
        if rows_processed is None: rows_processed = 0

        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_LOGS_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            with engine.connect() as conn:
                # Эта часть кода подключается к базе данных LOGS и выполняет одну команду UPDATE
                # end_time: устанавливает время завершения
                # status: меняет статус с STARTED на SUCCESS
                # rows_processed: записывает итоговое количество загруженных строк
                # message: добавляет сообщение об успешном завершении
                stmt = text(
                'UPDATE "LOGS"."ETL_LOGS" SET end_time = :end_time, status = :status, rows_processed = :rows, message = \'All files processed successfully.\' WHERE log_id = :id;')
                conn.execute(stmt,
                                 {'end_time': datetime.now(), 'status': 'SUCCESS', 'rows': rows_processed, 'id': log_id})
            logging.info(f"Process with log ID {log_id} completed successfully.")
        except Exception as e:
            logging.error(f"Error during final logging: {e}")

# Производим вызов наших функций, определяем порядок выполнения,
# благодаря оператору >> новая часть не начнется, если старая выполнена с ошибкой
    start_log_task = start_log()
    etl_task = extract_transform_load()
    finish_log_task = finish_log()

    start_log_task >> etl_task >> finish_log_task

bank_etl_dag_instance = bank_etl_dag()