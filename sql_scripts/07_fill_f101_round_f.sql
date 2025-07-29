-- ФИНАЛЬНАЯ РАБОЧАЯ ВЕРСИЯ
CREATE OR REPLACE PROCEDURE "DM".fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    report_start_date DATE;
    report_end_date DATE;
    log_message TEXT;
    rows_inserted INT;
BEGIN
    -- Определяем начало и конец отчетного месяца на основе входной даты
    -- (i_OnDate - это первый день месяца, СЛЕДУЮЩЕГО за отчетным)
    report_start_date := date_trunc('month', i_OnDate - INTERVAL '1 month');
    report_end_date := date_trunc('month', i_OnDate) - INTERVAL '1 day';


    -- --- ЛОГИРОВАНИЕ: НАЧАЛО ---
    log_message := 'Расчет формы 101 за период с ' || report_start_date::TEXT || ' по ' || report_end_date::TEXT;
    INSERT INTO "LOGS"."ETL_LOGS" (process_name, start_time, status, message)
    VALUES ('fill_f101_round_f', NOW(), 'STARTED', log_message);

    -- Удаляем старые данные за этот же период, чтобы избежать дублей при перезапуске
    DELETE FROM "DM"."DM_F101_ROUND_F" WHERE from_date = report_start_date;

    -- Основной запрос для расчета и вставки данных
    INSERT INTO "DM"."DM_F101_ROUND_F" (
        from_date, to_date, chapter, ledger_account, characteristic,
        balance_in_rub, balance_in_val, balance_in_total,
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    -- Используем CTE (Common Table Expressions) для пошагового расчета
    WITH 
    -- 1. Собираем информацию по всем счетам, которые были активны в отчетном периоде
    accounts_info AS (
        SELECT DISTINCT
            acc.account_rk,
            SUBSTRING(acc.account_number, 1, 5) AS ledger_account,
            acc.char_type AS characteristic,
            led.chapter,
            -- Определяем тип валюты для разделения на рубли и валюту
            CASE WHEN acc.currency_code IN ('643', '810') THEN 'RUB' ELSE 'VAL' END AS currency_type
        FROM "DS"."MD_ACCOUNT_D" acc
        JOIN "DS"."MD_LEDGER_ACCOUNT_S" led ON SUBSTRING(acc.account_number, 1, 5)::bigint = led.ledger_account
        WHERE report_end_date BETWEEN acc.data_actual_date AND acc.data_actual_end_date
          AND report_end_date BETWEEN led.start_date AND led.end_date
    ),
    -- 2. Считаем входящие остатки (остатки на конец предыдущего дня)
    opening_balances AS (
        SELECT
            ai.ledger_account,
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN b.balance_out_rub ELSE 0 END) AS balance_in_val
        FROM "DM"."DM_ACCOUNT_BALANCE_F" b
        JOIN accounts_info ai ON ai.account_rk = b.account_rk
        WHERE b.on_date = report_start_date - INTERVAL '1 day'
        GROUP BY ai.ledger_account
    ),
    -- 3. Считаем обороты за весь отчетный период
    turnovers AS (
        SELECT
            ai.ledger_account,
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val
        FROM "DM"."DM_ACCOUNT_TURNOVER_F" t
        JOIN accounts_info ai ON ai.account_rk = t.account_rk
        WHERE t.on_date BETWEEN report_start_date AND report_end_date
        GROUP BY ai.ledger_account
    ),
    -- 4. Считаем исходящие остатки (остатки на конец последнего дня отчетного периода)
    closing_balances AS (
        SELECT
            ai.ledger_account,
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN b.balance_out_rub ELSE 0 END) AS balance_out_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN b.balance_out_rub ELSE 0 END) AS balance_out_val
        FROM "DM"."DM_ACCOUNT_BALANCE_F" b
        JOIN accounts_info ai ON ai.account_rk = b.account_rk
        WHERE b.on_date = report_end_date
        GROUP BY ai.ledger_account
    )
    -- 5. Финальное объединение всех рассчитанных данных
    SELECT
        report_start_date,
        report_end_date,
        MIN(ai.chapter), -- Используем MIN, так как chapter одинаковый для одного ledger_account
        ai.ledger_account,
        MIN(ai.characteristic), -- Используем MIN, так как characteristic одинаковый для одного ledger_account
        -- Входящие остатки
        COALESCE(ob.balance_in_rub, 0),
        COALESCE(ob.balance_in_val, 0),
        COALESCE(ob.balance_in_rub, 0) + COALESCE(ob.balance_in_val, 0),
        -- Обороты по дебету
        COALESCE(t.turn_deb_rub, 0),
        COALESCE(t.turn_deb_val, 0),
        COALESCE(t.turn_deb_rub, 0) + COALESCE(t.turn_deb_val, 0),
        -- Обороты по кредиту
        COALESCE(t.turn_cre_rub, 0),
        COALESCE(t.turn_cre_val, 0),
        COALESCE(t.turn_cre_rub, 0) + COALESCE(t.turn_cre_val, 0),
        -- Исходящие остатки
        COALESCE(cb.balance_out_rub, 0),
        COALESCE(cb.balance_out_val, 0),
        COALESCE(cb.balance_out_rub, 0) + COALESCE(cb.balance_out_val, 0)
    FROM accounts_info ai
    LEFT JOIN opening_balances ob ON ai.ledger_account = ob.ledger_account
    LEFT JOIN turnovers t ON ai.ledger_account = t.ledger_account
    LEFT JOIN closing_balances cb ON ai.ledger_account = cb.ledger_account
    GROUP BY ai.ledger_account, ob.balance_in_rub, ob.balance_in_val, t.turn_deb_rub, t.turn_deb_val, t.turn_cre_rub, t.turn_cre_val, cb.balance_out_rub, cb.balance_out_val;

    -- --- ЛОГИРОВАНИЕ: ЗАВЕРШЕНИЕ ---
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    UPDATE "LOGS"."ETL_LOGS"
    SET end_time = NOW(), status = 'SUCCESS', rows_processed = rows_inserted, message = 'Расчет формы 101 успешно завершен'
    WHERE log_id = (SELECT MAX(log_id) FROM "LOGS"."ETL_LOGS" WHERE process_name = 'fill_f101_round_f' AND status = 'STARTED');

END;
$$;
