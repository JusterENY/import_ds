CREATE TABLE IF NOT EXISTS ds.md_currency_d (
    currency_rk				INT not null,
	data_actual_date		DATE not null,
    data_actual_end_date	DATE,
    currency_code			VARCHAR(3),
	code_iso_char			VARCHAR(3),
    PRIMARY KEY				(currency_rk,data_actual_date)
);

MERGE INTO ds.md_currency_d AS d 
USING (SELECT 
		"CURRENCY_RK" AS currency_rk,
    	to_date("DATA_ACTUAL_DATE",'YYYY-MM-DD') AS data_actual_date,
   		to_date("DATA_ACTUAL_END_DATE",'YYYY-MM-DD') AS data_actual_end_date,
    	"CURRENCY_CODE" AS currency_code,
    	"CODE_ISO_CHAR" AS code_iso_char
		FROM stage.md_currency_d
 		WHERE "CURRENCY_RK" IS NOT NULL
			AND "DATA_ACTUAL_DATE" IS NOT NULL) AS s
ON d.data_actual_date = s.data_actual_date AND d.currency_rk = s.currency_rk
WHEN MATCHED THEN UPDATE 
	SET data_actual_end_date = s.data_actual_end_date, currency_code = s.currency_code, code_iso_char = s.code_iso_char
WHEN NOT MATCHED THEN 
	INSERT (currency_rk,data_actual_date,data_actual_end_date,currency_code,code_iso_char)
	VALUES (s.currency_rk,s.data_actual_date,s.data_actual_end_date,s.currency_code,s.code_iso_char);
