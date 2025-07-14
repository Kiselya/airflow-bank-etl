-- (ИСПРАВЛЕННАЯ ВЕРСИЯ)
CREATE OR REPLACE PROCEDURE "DM".fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    log_message TEXT;
    report_start_date DATE;
    report_end_date DATE;
BEGIN
    -- Определяем даты отчетного периода
    report_start_date := date_trunc('month', i_OnDate - INTERVAL '1 month');
    report_end_date := date_trunc('month', i_OnDate) - INTERVAL '1 day';

    -- Логирование начала
    log_message := 'Filling DM_F101_ROUND_F for ' || to_char(report_start_date, 'YYYY-MM');
    INSERT INTO "LOGS"."ETL_LOGS" (process_name, start_time, status, message)
    VALUES ('fill_f101_round_f', NOW(), 'STARTED', log_message);

    -- Удаление старых данных за этот период
    DELETE FROM "DM"."DM_F101_ROUND_F" WHERE from_date = report_start_date;

    -- Основной запрос для расчета и вставки данных
    INSERT INTO "DM"."DM_F101_ROUND_F"
    WITH accounts_info AS (
        -- Собираем информацию по всем счетам, которые были активны в отчетном периоде
        SELECT DISTINCT
            acc.account_rk,
            SUBSTRING(acc.account_number, 1, 5) AS ledger_account,
            acc.char_type AS characteristic,
            led.chapter,
            CASE WHEN acc.currency_code IN ('643', '810') THEN 'RUB' ELSE 'VAL' END AS currency_type
        FROM "DS"."MD_ACCOUNT_D" acc
        JOIN "DS"."MD_LEDGER_ACCOUNT_S" led ON SUBSTRING(acc.account_number, 1, 5)::bigint = led.ledger_account
        -- ИЗМЕНЕНИЕ ЗДЕСЬ: Проверяем актуальность на конец отчетного месяца, а не на следующий день
        WHERE report_end_date BETWEEN acc.data_actual_date AND acc.data_actual_end_date
          AND report_end_date BETWEEN led.start_date AND led.end_date
    ),
    opening_balances AS (
        -- Считаем входящие остатки
        SELECT
            ai.ledger_account,
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN b.balance_out_rub ELSE 0 END) AS balance_in_val
        FROM "DM"."DM_ACCOUNT_BALANCE_F" b
        JOIN accounts_info ai ON ai.account_rk = b.account_rk
        WHERE b.on_date = report_start_date - INTERVAL '1 day'
        GROUP BY ai.ledger_account
    ),
    turnovers AS (
        -- Считаем обороты за период
        SELECT
            ai.ledger_account,
            SUM(t.debet_amount_rub) AS turn_deb_total,
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
            SUM(t.credit_amount_rub) AS turn_cre_total,
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val
        FROM "DM"."DM_ACCOUNT_TURNOVER_F" t
        JOIN accounts_info ai ON ai.account_rk = t.account_rk
        WHERE t.on_date BETWEEN report_start_date AND report_end_date
        GROUP BY ai.ledger_account
    ),
    closing_balances AS (
        -- Считаем исходящие остатки
        SELECT
            ai.ledger_account,
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN b.balance_out_rub ELSE 0 END) AS balance_out_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN b.balance_out_rub ELSE 0 END) AS balance_out_val
        FROM "DM"."DM_ACCOUNT_BALANCE_F" b
        JOIN accounts_info ai ON ai.account_rk = b.account_rk
        WHERE b.on_date = report_end_date
        GROUP BY ai.ledger_account
    )
    -- Финальное объединение всех рассчитанных данных
    SELECT
        report_start_date,
        report_end_date,
        MIN(ai.chapter),
        ai.ledger_account,
        MIN(ai.characteristic),
        COALESCE(ob.balance_in_rub, 0),
        COALESCE(ob.balance_in_val, 0),
        COALESCE(ob.balance_in_rub, 0) + COALESCE(ob.balance_in_val, 0),
        COALESCE(t.turn_deb_rub, 0),
        COALESCE(t.turn_deb_val, 0),
        COALESCE(t.turn_deb_total, 0),
        COALESCE(t.turn_cre_rub, 0),
        COALESCE(t.turn_cre_val, 0),
        COALESCE(t.turn_cre_total, 0),
        COALESCE(cb.balance_out_rub, 0),
        COALESCE(cb.balance_out_val, 0),
        COALESCE(cb.balance_out_rub, 0) + COALESCE(cb.balance_out_val, 0)
    FROM accounts_info ai
    LEFT JOIN opening_balances ob ON ai.ledger_account = ob.ledger_account
    LEFT JOIN turnovers t ON ai.ledger_account = t.ledger_account
    LEFT JOIN closing_balances cb ON ai.ledger_account = cb.ledger_account
    GROUP BY ai.ledger_account, ob.balance_in_rub, ob.balance_in_val, t.turn_deb_rub, t.turn_deb_val, t.turn_deb_total, t.turn_cre_rub, t.turn_cre_val, t.turn_cre_total, cb.balance_out_rub, cb.balance_out_val;

    -- Логирование окончания
    UPDATE "LOGS"."ETL_LOGS"
    SET end_time = NOW(), status = 'SUCCESS'
    WHERE process_name = 'fill_f101_round_f' AND start_time = (
        SELECT MAX(start_time) FROM "LOGS"."ETL_LOGS" WHERE process_name = 'fill_f101_round_f'
    );

END;
$$;