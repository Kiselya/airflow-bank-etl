-- Создание копии таблицы для импорта измененных данных F101

-- Создаем копию таблицы DM_F101_ROUND_F для задания 1.4
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

-- Выдаем права пользователю ds_user на новую таблицу
GRANT ALL PRIVILEGES ON TABLE "DM"."DM_F101_ROUND_F_V2" TO ds_user;