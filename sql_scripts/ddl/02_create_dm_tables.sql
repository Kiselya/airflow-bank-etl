CREATE SCHEMA IF NOT EXISTS "DM";

CREATE TABLE "DM"."DM_ACCOUNT_TURNOVER_F" (
    on_date DATE NOT NULL,
    account_rk BIGINT NOT NULL,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8)
);

CREATE TABLE "DM"."DM_ACCOUNT_BALANCE_F" (
    on_date DATE NOT NULL,
    account_rk BIGINT NOT NULL,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8)
);