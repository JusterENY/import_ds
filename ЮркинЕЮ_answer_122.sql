DROP TABLE IF EXISTS dm.dm_account_balance_f;

CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f (
	on_date 		DATE,
	account_rk 		INT,
	balance_out		DECIMAL(23,8),
	balance_out_rub	DECIMAL(23,8)
);

INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out,	balance_out_rub)
SELECT 
	db.on_date, 
	db.account_rk, 
	db.balance_out, 
	db.balance_out * COALESCE(c.reduced_cource,1.0) AS balance_out_rub
FROM ds.ft_balance_f db
LEFT JOIN (
		SELECT de.data_actual_date, de.data_actual_end_date, da.account_rk, de.reduced_cource, da.char_type
		FROM ds.md_exchange_rate_d de
		INNER JOIN ds.md_account_d da ON de.currency_rk = da.currency_rk 
										AND de.data_actual_end_date between da.data_actual_date AND da.data_actual_end_date					
		) c ON c.account_rk = db.account_rk AND db.on_date between c.data_actual_date AND c.data_actual_end_date;

SELECT * FROM dm.dm_account_balance_f;

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f (i_OnDate date) 
LANGUAGE plpgsql 
AS $$
DECLARE
  OnDate date;
BEGIN  
INSERT INTO logs.log_dm (date_import,name_table,status)
	VALUES (NOW(), 'dm.dm_account_balance_f',0);

DELETE FROM dm.dm_account_balance_f WHERE on_date > '2017-12-31';

FOR OnDate IN SELECT generate_series('2018-01-01', i_OnDate, '1 day'::interval)::date 
  LOOP
	INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out,	balance_out_rub)
	SELECT 
		OnDate AS on_date,
		da.account_rk,
		COALESCE(dab.balance_out,0.0) + COALESCE(dat.debet_amount,0.0) - COALESCE(dat.credit_amount,0.0) AS balance_out,
		COALESCE(dab.balance_out_rub,0.0) + COALESCE(dat.debet_amount_rub,0.0) - COALESCE(dat.credit_amount_rub,0.0) AS balance_out_rub
	FROM ds.md_account_d da
	LEFT JOIN dm.dm_account_balance_f dab ON da.account_rk = dab.account_rk AND dab.on_date = OnDate - INTERVAL '1 day'
	LEFT JOIN dm.dm_account_turnover_f dat ON da.account_rk = dat.account_rk AND dat.on_date = OnDate
	WHERE OnDate BETWEEN data_actual_date AND data_actual_end_date
	AND char_type = 'A';

	INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out,	balance_out_rub)
	SELECT 
		OnDate AS on_date,
		da.account_rk,
		COALESCE(dab.balance_out,0.0) - COALESCE(dat.debet_amount,0.0) + COALESCE(dat.credit_amount,0.0) AS balance_out,
		COALESCE(dab.balance_out_rub,0.0) - COALESCE(dat.debet_amount_rub,0.0) + COALESCE(dat.credit_amount_rub,0.0) AS balance_out_rub
	FROM ds.md_account_d da
	LEFT JOIN dm.dm_account_balance_f dab ON da.account_rk = dab.account_rk AND dab.on_date = OnDate - INTERVAL '1 day'
	LEFT JOIN dm.dm_account_turnover_f dat ON da.account_rk = dat.account_rk AND dat.on_date = OnDate
	WHERE OnDate BETWEEN data_actual_date AND data_actual_end_date
	AND char_type = 'П';

  END LOOP;

INSERT INTO logs.log_dm (date_import,name_table,status)
	VALUES (NOW(), 'dm.dm_account_balance_f',1);  
END;
$$;

CALL ds.fill_account_balance_f ('2018-01-31')
SELECT * FROM dm.dm_account_balance_f 
SELECT * FROM dm.dm_account_turnover_f

/*Так как остатки за день считаются на основе остатков за предыдущий день, вам необходимо заполнить витрину DM.DM_ACCOUNT_BALANCE_F за 31.12.2017 
данными из DS.FT_BALANCE_F. Поля on_date, account_rk, balance_out заполняются один в один, 
поле balance_out_rub заполняем как balance_out, умноженный на курс действующий за 31.12.2017. Если информации о курсе нет, то умножаем на единицу.

Затем необходимо создать процедуру заполнения витрины остатков по лицевым счетам. Назовите ее ds.fill_account_balance_f.  
Данная процедура должна иметь один входной параметр – дату расчета (i_OnDate). Алгоритм заполнения следующий:
необходимо взять все счета, действующие за дату расчета (дата расчета лежит между датами актуальности записей в таблице DS.MD_ACCOUNT_D), 
для этих счетов рассчитываем balance_out по следующему алгоритму:
·для активных счетов (DS.MD_ACCOUNT_D.char_type = ‘А’): берем остаток в валюте счета за предыдущий день (если его нет, то считаем его равным 0), 
прибавляем к нему обороты по дебету в валюте счета (DM.DM_ACCOUNT_TURNOVER_F.debet_amount) и вычитаем обороты по кредиту в валюте счета (DM.DM_ACCOUNT_TURNOVER_F.credit_amount) за этот день.
·для пассивных счетов (DS.MD_ACCOUNT_D.char_type = ‘П’): берем остаток в валюте счета за предыдущий день (если его нет, то считаем его равным 0), 
вычитаем из него обороты по дебету в валюте счета и прибавляем обороты по кредиту в валюте счета за этот день.

Поле balance_out_rub заполняем аналогично полю balance_out, но для расчета берем поля в рублях. 
Обратите внимание, что в какие-то дни по счету может не быть оборотов, но остаток по счету мы должны заполнить. 
После создания процедуры рассчитайте витрину остатков за каждый день января 2018 года.*/
