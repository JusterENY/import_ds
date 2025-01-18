CREATE TABLE IF NOT EXISTS ds.md_account_d (
    data_actual_date        DATE not null,
    data_actual_end_date    DATE not null,
    account_rk              INT not null,
    account_number          VARCHAR(20) not null,
    char_type               VARCHAR(1) not null,
    currency_rk             INT not null,
    currency_code           VARCHAR(3) not null,
    PRIMARY KEY             (data_actual_date, account_rk)
);

MERGE INTO ds.md_account_d AS d 
USING (SELECT 
    to_date("DATA_ACTUAL_DATE",'YYYY-MM-DD') AS data_actual_date,
    to_date("DATA_ACTUAL_END_DATE",'YYYY-MM-DD') AS data_actual_end_date,
    "ACCOUNT_RK" AS account_rk,
    "ACCOUNT_NUMBER" AS account_number,
    "CHAR_TYPE" AS char_type,
    "CURRENCY_RK" AS currency_rk,
    "CURRENCY_CODE" AS currency_code
		FROM stage.md_account_d
 		WHERE "DATA_ACTUAL_DATE" IS NOT NULL
            AND "DATA_ACTUAL_END_DATE" IS NOT NULL
            AND "ACCOUNT_RK" IS NOT NULL
            AND "ACCOUNT_NUMBER" IS NOT NULL
            AND "CHAR_TYPE" IS NOT NULL
   			AND "CURRENCY_RK" IS NOT NULL
            AND "CURRENCY_CODE" IS NOT NULL) AS s
ON d.data_actual_date = s.data_actual_date AND d.account_rk = s.account_rk
WHEN MATCHED THEN UPDATE 
	SET data_actual_end_date = s.data_actual_end_date, account_number = s.account_number, char_type = s.char_type, currency_rk = s.currency_rk, currency_code = s.currency_code
WHEN NOT MATCHED THEN 
	INSERT (data_actual_date,data_actual_end_date,account_rk,account_number,char_type,currency_rk,currency_code)
	VALUES (s.data_actual_date,s.data_actual_end_date,s.account_rk,s.account_number,s.char_type,s.currency_rk,s.currency_code);
