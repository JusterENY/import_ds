CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
	on_date DATE not null, 
	account_rk INT not null, 
	currency_rk INT, 
	balance_out FLOAT, 
	PRIMARY KEY(on_date, account_rk)
);

MERGE INTO ds.ft_balance_f AS dsb 
USING (SELECT 
		 to_date("ON_DATE" , 'DD-MM-YYYY') AS on_date
		,"ACCOUNT_RK" AS account_rk
		,"CURRENCY_RK" AS currency_rk
		,"BALANCE_OUT" AS balance_out
		FROM stage.ft_balance_f
 		WHERE "ACCOUNT_RK"  IS NOT NULL
   			AND "ON_DATE" IS NOT NULL) AS stb
ON dsb.on_date = stb.on_date AND dsb.account_rk = stb.account_rk
WHEN MATCHED THEN UPDATE 
	SET currency_rk = stb.currency_rk, balance_out = stb.balance_out
WHEN NOT MATCHED THEN 
	INSERT (on_date,account_rk,currency_rk,balance_out)
	VALUES (stb.on_date,stb.account_rk,stb.currency_rk,stb.balance_out);
