-- ФИНАЛЬНАЯ РАБОЧАЯ ВЕРСИЯ
CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
BEGIN
    -- Удаляем данные только за тот день, который будем считать
    DELETE FROM "DM"."DM_ACCOUNT_TURNOVER_F" WHERE on_date = i_OnDate;

    -- Вставляем рассчитанные данные
    INSERT INTO "DM"."DM_ACCOUNT_TURNOVER_F" (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    
    -- Используем CTE для предварительной агрегации оборотов
    WITH turnovers_by_account AS (
        -- Сначала агрегируем все кредитовые обороты за день
        SELECT
            p.credit_account_rk AS account_rk,
            SUM(p.credit_amount) AS credit_amount,
            0.0 AS debet_amount
        FROM "DS"."FT_POSTING_F" p
        WHERE p.oper_date = i_OnDate
        GROUP BY p.credit_account_rk

        UNION ALL

        -- Затем агрегируем все дебетовые обороты за день
        SELECT
            p.debet_account_rk AS account_rk,
            0.0 AS credit_amount,
            SUM(p.debet_amount) AS debet_amount
        FROM "DS"."FT_POSTING_F" p
        WHERE p.oper_date = i_OnDate
        GROUP BY p.debet_account_rk
    )
    -- Финальная сборка, соединение со справочниками и расчет рублевого эквивалента
    SELECT
        i_OnDate,
        t.account_rk,
        SUM(t.credit_amount),
        SUM(t.credit_amount * COALESCE(er.reduced_cource, 1)),
        SUM(t.debet_amount),
        SUM(t.debet_amount * COALESCE(er.reduced_cource, 1))
    FROM turnovers_by_account t
    -- Соединяем со счетами, чтобы убедиться, что счет актуален на дату оборота
    JOIN "DS"."MD_ACCOUNT_D" acc
        ON acc.account_rk = t.account_rk
        AND i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date
    -- Соединяем с курсами валют, чтобы получить курс на дату оборота
    LEFT JOIN "DS"."MD_EXCHANGE_RATE_D" er
        ON er.currency_rk = acc.currency_rk
        AND i_OnDate BETWEEN er.data_actual_date AND er.data_actual_end_date
    -- Группируем по дате и счету для финального суммирования
    GROUP BY
        i_OnDate, t.account_rk;
END;
$$;
