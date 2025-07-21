-- Файл: 00_create_logs_infrastructure.sql

-- Создаем схему для логов
CREATE SCHEMA IF NOT EXISTS "LOGS";

-- Создаем таблицу для логов
CREATE TABLE IF NOT EXISTS "LOGS"."ETL_LOGS" (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50),
    rows_processed INT,
    message TEXT
);