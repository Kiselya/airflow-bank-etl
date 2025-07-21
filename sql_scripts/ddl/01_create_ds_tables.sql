### Создаем схему для детального слоя
CREATE SCHEMA IF NOT EXISTS "DS";

### Создание таблиц детального слоя
CREATE TABLE "DS"."MD_ACCOUNT_D" (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk BIGINT NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(30) NOT NULL,
    currency_rk BIGINT NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    PRIMARY KEY (account_rk, data_actual_date)
);

CREATE TABLE "DS"."MD_CURRENCY_D" (
    currency_rk BIGINT NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE "DS"."MD_EXCHANGE_RATE_D" (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk BIGINT NOT NULL,
    reduced_cource NUMERIC,
    code_iso_num VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE "DS"."MD_LEDGER_ACCOUNT_S" (
    ledger_account BIGINT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    chapter VARCHAR(30),
    chapter_name TEXT,
    section_number INT,
    section_name TEXT,
    subsection_name TEXT,
    ledger1_account BIGINT,
    ledger1_account_name TEXT,
    ledger_account_name TEXT,
    characteristic VARCHAR(30),
    PRIMARY KEY (ledger_account, start_date)
);

CREATE TABLE "DS"."FT_POSTING_F" (
    oper_date DATE NOT NULL,
    credit_account_rk BIGINT NOT NULL,
    debet_account_rk BIGINT NOT NULL,
    credit_amount NUMERIC,
    debet_amount NUMERIC
);

CREATE TABLE "DS"."FT_BALANCE_F" (
    on_date DATE NOT NULL,
    account_rk BIGINT NOT NULL,
    currency_rk BIGINT,
    balance_out NUMERIC,
    PRIMARY KEY (on_date, account_rk)
);