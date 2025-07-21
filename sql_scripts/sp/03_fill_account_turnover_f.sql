CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    log_message TEXT;
BEGIN
    ### Логируем начало
    log_message := 'Filling DM_ACCOUNT_TURNOVER_F for ' || i_OnDate::TEXT;
    INSERT INTO "LOGS"."ETL_LOGS" (process_name, start_time, status, message)
    VALUES ('fill_account_turnover_f', NOW(), 'STARTED', log_message);

    ### Удаляем старые данные за дату расчета
    DELETE FROM "DM"."DM_ACCOUNT_TURNOVER_F" WHERE on_date = i_OnDate;

    ### Рассчитываем и вставляем новые данные
    INSERT INTO "DM"."DM_ACCOUNT_TURNOVER_F" (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    WITH all_turnovers AS (
        ### Собираем кредитные обороты
        SELECT
            p.oper_date,
            p.credit_account_rk AS account_rk,
            p.credit_amount AS credit_amount,
            0.0 AS debet_amount,
            acc.currency_rk
        FROM "DS"."FT_POSTING_F" p
        JOIN "DS"."MD_ACCOUNT_D" acc ON acc.account_rk = p.credit_account_rk
        WHERE p.oper_date = i_OnDate
        
        UNION ALL
        
        ### Собираем дебетовые обороты
        SELECT
            p.oper_date,
            p.debet_account_rk AS account_rk,
            0.0 AS credit_amount,
            p.debet_amount AS debet_amount,
            acc.currency_rk
        FROM "DS"."FT_POSTING_F" p
        JOIN "DS"."MD_ACCOUNT_D" acc ON acc.account_rk = p.debet_account_rk
        WHERE p.oper_date = i_OnDate
    )
    SELECT
        t.oper_date,
        t.account_rk,
        SUM(t.credit_amount) AS credit_amount,
        SUM(t.credit_amount * COALESCE(er.reduced_cource, 1)) AS credit_amount_rub,
        SUM(t.debet_amount) AS debet_amount,
        SUM(t.debet_amount * COALESCE(er.reduced_cource, 1)) AS debet_amount_rub
    FROM all_turnovers t
    LEFT JOIN "DS"."MD_EXCHANGE_RATE_D" er ON er.currency_rk = t.currency_rk AND i_OnDate BETWEEN er.data_actual_date AND er.data_actual_end_date
    GROUP BY t.oper_date, t.account_rk;

    ### Логируем конец
    UPDATE "LOGS"."ETL_LOGS"
    SET end_time = NOW(), status = 'SUCCESS'
    WHERE process_name = 'fill_account_turnover_f' AND start_time = (
        SELECT MAX(start_time) FROM "LOGS"."ETL_LOGS" WHERE process_name = 'fill_account_turnover_f'
    );

END;
$$;