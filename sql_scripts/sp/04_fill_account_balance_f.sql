CREATE OR REPLACE PROCEDURE "DS".fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    log_message TEXT;
BEGIN
    ### Логирование начала
    log_message := 'Filling DM_ACCOUNT_BALANCE_F for ' || i_OnDate::TEXT;
    INSERT INTO "LOGS"."ETL_LOGS" (process_name, start_time, status, message)
    VALUES ('fill_account_balance_f', NOW(), 'STARTED', log_message);

    ### Удаление старых данных
    DELETE FROM "DM"."DM_ACCOUNT_BALANCE_F" WHERE on_date = i_OnDate;

    ### Расчет и вставка
    INSERT INTO "DM"."DM_ACCOUNT_BALANCE_F" (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate,
        acc.account_rk,
        ### Расчет остатка в валюте счета
        CASE
            WHEN acc.char_type = 'А' THEN COALESCE(prev_b.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            WHEN acc.char_type = 'П' THEN COALESCE(prev_b.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
        END AS balance_out,
        ### Расчет остатка в рублях
        CASE
            WHEN acc.char_type = 'А' THEN COALESCE(prev_b.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
            WHEN acc.char_type = 'П' THEN COALESCE(prev_b.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM "DS"."MD_ACCOUNT_D" acc
    LEFT JOIN "DM"."DM_ACCOUNT_BALANCE_F" prev_b ON prev_b.account_rk = acc.account_rk AND prev_b.on_date = i_OnDate - INTERVAL '1 day'
    LEFT JOIN "DM"."DM_ACCOUNT_TURNOVER_F" t ON t.account_rk = acc.account_rk AND t.on_date = i_OnDate
    WHERE i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;

    ### Логирование конца
    UPDATE "LOGS"."ETL_LOGS"
    SET end_time = NOW(), status = 'SUCCESS'
    WHERE process_name = 'fill_account_balance_f' AND start_time = (
        SELECT MAX(start_time) FROM "LOGS"."ETL_LOGS" WHERE process_name = 'fill_account_balance_f'
    );
END;
$$;