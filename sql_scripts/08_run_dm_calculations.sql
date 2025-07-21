-- Файл: 08_run_dm_calculations.sql
-- Этот скрипт запускает расчет промежуточных витрин DM (обороты и остатки)

-- ШАГ 1: Первоначальное заполнение остатков на 31.12.2017
-- Мы берем данные из детального слоя и рассчитываем рублевый эквивалент
INSERT INTO "DM"."DM_ACCOUNT_BALANCE_F" (on_date, account_rk, balance_out, balance_out_rub)
SELECT
    b.on_date,
    b.account_rk,
    b.balance_out,
    b.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
FROM "DS"."FT_BALANCE_F" b
LEFT JOIN "DS"."MD_EXCHANGE_RATE_D" er 
    ON er.currency_rk = b.currency_rk 
    AND b.on_date BETWEEN er.data_actual_date AND er.data_actual_end_date
WHERE b.on_date = '2017-12-31';

-- ШАГ 2: Расчет оборотов за январь 2018
-- Запускаем процедуру fill_account_turnover_f в цикле на каждый день месяца
DO $$
DECLARE
    calc_date DATE;
BEGIN
    FOR calc_date IN (SELECT generate_series('2018-01-01'::DATE, '2018-01-31'::DATE, '1 day'))
    LOOP
        CALL "DS".fill_account_turnover_f(calc_date);
    END LOOP;
END;
$$;

-- ШАГ 3: Расчет остатков за январь 2018
-- Запускаем процедуру fill_account_balance_f в цикле на каждый день месяца
DO $$
DECLARE
    calc_date DATE;
BEGIN
    FOR calc_date IN (SELECT generate_series('2018-01-01'::DATE, '2018-01-31'::DATE, '1 day'))
    LOOP
        CALL "DS".fill_account_balance_f(calc_date);
    END LOOP;
END;
$$;
