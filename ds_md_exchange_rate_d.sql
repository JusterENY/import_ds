CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
	data_actual_date		DATE not null,
    data_actual_end_date	DATE,
    currency_rk				INT not null,	
    reduced_cource			NUMERIC(19,2),
	code_iso_num			VARCHAR(3),
    PRIMARY KEY				(data_actual_date,currency_rk)
);

MERGE INTO ds.md_exchange_rate_d AS d 
USING (SELECT DISTINCT
    	to_date("DATA_ACTUAL_DATE",'YYYY-MM-DD') AS data_actual_date,
   		to_date("DATA_ACTUAL_END_DATE",'YYYY-MM-DD') AS data_actual_end_date,
		"CURRENCY_RK" AS currency_rk,
    	"REDUCED_COURCE" AS reduced_cource,
    	"CODE_ISO_NUM" AS code_iso_num
		FROM stage.md_exchange_rate_d
 		WHERE "CURRENCY_RK" IS NOT NULL
		 	AND "DATA_ACTUAL_DATE" IS NOT NULL) AS s
ON d.data_actual_date = s.data_actual_date AND d.currency_rk = s.currency_rk
WHEN MATCHED THEN UPDATE 
	SET data_actual_end_date = s.data_actual_end_date, reduced_cource = s.reduced_cource, code_iso_num = s.code_iso_num
WHEN NOT MATCHED THEN 
	INSERT (data_actual_date,data_actual_end_date,currency_rk,reduced_cource,code_iso_num)
	VALUES (s.data_actual_date,s.data_actual_end_date,s.currency_rk,s.reduced_cource,s.code_iso_num);