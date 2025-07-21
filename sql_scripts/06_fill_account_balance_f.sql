-- ФИНАЛЬНАЯ РАБОЧАЯ ВЕРСИЯ
CREATE OR REPLACE PROCEDURE "DS".fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
BEGIN
    -- Удаляем данные только за тот день, который будем считать
    DELETE FROM "DM"."DM_ACCOUNT_BALANCE_F" WHERE on_date = i_OnDate;

    -- Вставляем рассчитанные данные
    INSERT INTO "DM"."DM_ACCOUNT_BALANCE_F" (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate,
        acc.account_rk,
        
        -- Расчет остатка в валюте счета. Логика: Остаток_вчера +/- Обороты_сегодня
        CASE
            WHEN acc.char_type = 'А' THEN COALESCE(prev_b.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            ELSE COALESCE(prev_b.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
        END AS balance_out,

        -- ИСПРАВЛЕННЫЙ РАСЧЕТ: Расчет остатка в рублях.
        -- Логика: Рублевый_остаток_вчера +/- Рублевые_обороты_сегодня
        CASE
            WHEN acc.char_type = 'А' THEN COALESCE(prev_b.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
            ELSE COALESCE(prev_b.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub

    FROM "DS"."MD_ACCOUNT_D" acc
    -- Присоединяем остаток за предыдущий день
    LEFT JOIN "DM"."DM_ACCOUNT_BALANCE_F" prev_b 
        ON prev_b.account_rk = acc.account_rk AND prev_b.on_date = i_OnDate - INTERVAL '1 day'
    -- Присоединяем обороты за текущий день
    LEFT JOIN "DM"."DM_ACCOUNT_TURNOVER_F" t 
        ON t.account_rk = acc.account_rk AND t.on_date = i_OnDate
    -- Выбираем только те счета, которые активны на дату расчета
    WHERE i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;
END;
$$;
