CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS logs.log_dm (
	date_import timestamp,
	name_table varchar(50),
	status int
);

DROP TABLE IF EXISTS dm.dm_account_turnover_f;

CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f (
	on_date 			DATE,
	account_rk 			INT,
	credit_amount		DECIMAL(23,8),
	credit_amount_rub	DECIMAL(23,8),
	debet_amount		DECIMAL(23,8),
	debet_amount_rub	DECIMAL(23,8)
);

--TRUNCATE TABLE dm.dm_account_turnover_f;

CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f (i_OnDate date) 
LANGUAGE SQL 
AS $$
INSERT INTO logs.log_dm (date_import,name_table,status)
	VALUES (NOW(), 'dm.dm_account_turnover_f',0);
	
DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;

WITH cource AS (
		SELECT de.data_actual_date, de.data_actual_end_date, da.account_rk, de.reduced_cource
		FROM ds.md_exchange_rate_d de
		INNER JOIN ds.md_account_d da ON de.currency_rk = da.currency_rk 
										AND de.data_actual_end_date between da.data_actual_date AND da.data_actual_end_date					
)
INSERT INTO dm.dm_account_turnover_f (
	on_date,
	account_rk,
	credit_amount,
	credit_amount_rub,
	debet_amount,
	debet_amount_rub   
) 
SELECT 
	on_date,
	account_rk,
	sum(credit_amount),
	sum(credit_amount_rub),
	sum(debet_amount),
	sum(debet_amount_rub) 
FROM (
    SELECT 
		dp.oper_date AS on_date, 
		dp.credit_account_rk AS account_rk, 
		SUM(dp.credit_amount) AS credit_amount, 
		SUM(dp.credit_amount * COALESCE(c.reduced_cource,1.0)) AS credit_amount_rub,
		0.0 AS debet_amount,
		0.0 AS debet_amount_rub
    FROM ds.ft_posting_f dp
	LEFT JOIN cource c ON c.account_rk = dp.credit_account_rk AND dp.oper_date BETWEEN c.data_actual_date AND c.data_actual_end_date
	WHERE oper_date = i_OnDate
	GROUP BY dp.oper_date, dp.credit_account_rk
	UNION ALL
	SELECT 
		dp.oper_date AS on_date, 
		dp.debet_account_rk AS account_rk, 
		0.0 AS credit_amount, 
		0.0 AS credit_amount_rub,
		SUM(dp.debet_amount) AS debet_amount,
		SUM(ROUND(dp.debet_amount * COALESCE(c.reduced_cource,1.0),2)) AS debet_amount_rub
    FROM ds.ft_posting_f dp
	LEFT JOIN cource c ON c.account_rk = dp.debet_account_rk AND dp.oper_date BETWEEN c.data_actual_date AND c.data_actual_end_date
	WHERE oper_date = i_OnDate
	GROUP BY dp.oper_date, dp.debet_account_rk
	)
GROUP BY on_date, account_rk;

INSERT INTO logs.log_dm (date_import,name_table,status)
	VALUES (NOW(), 'dm.dm_account_turnover_f',1);
$$;


CALL ds.fill_account_turnover_f('2018-01-10');
select * from dm.dm_account_turnover_f order by on_date;
--TRUNCATE TABLE dm.dm_account_turnover_f;

DO $$
DECLARE
  start_date date := '2018-01-01';
  end_date date := '2018-01-31';
  OnDate date;
BEGIN
  FOR OnDate IN SELECT generate_series(start_date, end_date, '1 day'::interval)::date LOOP
    CALL ds.fill_account_turnover_f(OnDate);
  END LOOP;
END $$;