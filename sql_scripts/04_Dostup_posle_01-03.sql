-- ФИНАЛЬНАЯ ВЕРСИЯ СКРИПТА ВЫДАЧИ ПРАВ

-- 1. Создаем пользователей, только если они еще не существуют
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ds_user') THEN
      CREATE ROLE ds_user LOGIN PASSWORD '0510';
   END IF;
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'logs_user') THEN
      CREATE ROLE logs_user LOGIN PASSWORD '0510';
   END IF;
END
$$;


-- 2. Права для logs_user
-- Разрешаем "использовать" схему LOGS
GRANT USAGE ON SCHEMA "LOGS" TO logs_user;
-- Выдаем права на конкретную таблицу логов
GRANT ALL PRIVILEGES ON TABLE "LOGS"."ETL_LOGS" TO logs_user;
-- Выдаем права на sequence для автоинкремента ID
GRANT USAGE, SELECT ON SEQUENCE "LOGS"."ETL_LOGS_log_id_seq" TO logs_user;


-- 3. Права для ds_user
-- Разрешаем "использовать" И "СОЗДАВАТЬ" объекты в схемах
GRANT USAGE, CREATE ON SCHEMA "DS" TO ds_user;
GRANT USAGE, CREATE ON SCHEMA "DM" TO ds_user;

-- Выдаем права на ВСЕ УЖЕ СУЩЕСТВУЮЩИЕ таблицы в схемах
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "DS" TO ds_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "DM" TO ds_user;

-- 4. Права на БУДУЩИЕ таблицы (чтобы Airflow мог создавать временные таблицы)
-- Эта команда гарантирует, что любые НОВЫЕ таблицы, которые будут созданы,
-- автоматически получат все права для пользователя ds_user.
ALTER DEFAULT PRIVILEGES IN SCHEMA "DS" GRANT ALL ON TABLES TO ds_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA "DM" GRANT ALL ON TABLES TO ds_user;

