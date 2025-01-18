CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f (
FROM_DATE 			DATE,
TO_DATE				DATE,
CHAPTER				CHAR(1),
LEDGER_ACCOUNT		CHAR(5),
CHARACTERISTIC		CHAR(1),
BALANCE_IN_RUB		DECIMAL(23,8),
R_BALANCE_IN_RUB	DECIMAL(23,8),
BALANCE_IN_VAL		DECIMAL(23,8),
R_BALANCE_IN_VAL	DECIMAL(23,8),
BALANCE_IN_TOTAL	DECIMAL(23,8),
R_BALANCE_IN_TOTAL	DECIMAL(23,8),
TURN_DEB_RUB		DECIMAL(23,8),
R_TURN_DEB_RUB		DECIMAL(23,8),
TURN_DEB_VAL		DECIMAL(23,8),
R_TURN_DEB_VAL		DECIMAL(23,8),
TURN_DEB_TOTAL		DECIMAL(23,8),
R_TURN_DEB_TOTAL	DECIMAL(23,8),
TURN_CRE_RUB		DECIMAL(23,8),
R_TURN_CRE_RUB		DECIMAL(23,8),
TURN_CRE_VAL		DECIMAL(23,8),
R_TURN_CRE_VAL		DECIMAL(23,8),
TURN_CRE_TOTAL		DECIMAL(23,8),
R_TURN_CRE_TOTAL	DECIMAL(23,8),
BALANCE_OUT_RUB		DECIMAL(23,8),
R_BALANCE_OUT_RUB	DECIMAL(23,8),
BALANCE_OUT_VAL		DECIMAL(23,8),
R_BALANCE_OUT_VAL	DECIMAL(23,8),
BALANCE_OUT_TOTAL	DECIMAL(23,8),
R_BALANCE_OUT_TOTAL	DECIMAL(23,8)
);
/* 101 форма содержит информацию об остатках и оборотах за отчетный период, сгруппированных по балансовым счетам второго порядка. 
Вам необходимо создать процедуру расчета (назовите ее dm.fill_f101_round_f), которая должна иметь один входной параметр – отчетную дату (i_OnDate). 
Отчетная дата – это первый день месяца, следующего за отчетным. То есть, если мы хотим рассчитать отчет за январь 2018 года, то должны передать в процедуру 1 февраля 2018 года. 
В отчет должна попасть информация по всем счетам, действующим в отчетном периоде, группировка в отчете идет по балансовым счетам второго порядка 
(балансовый счет второго порядка – это первые 5 символов номера счета (DS.MD_ACCOUNT_D.account_number). 
Поля витрины должны заполняться следующим образом:
FROM_DATE – первый день отчетного периода, 
TO_DATE – последний день отчетного периода;
CHAPTER – глава из справочника балансовых счетов (DS.MD_LEDGER_ACCOUNT_S);
LEDGER_ACCOUNT – балансовый счет второго порядка, 
CHARACTERISTIC – характеристика счета (можно получить из поля DS.MD_ACCOUNT_D.char_type);
BALANCE_IN_RUB – сумма остатков в рублях (DM.DM_ACCOUNT_BALANCE_F.balance_out_rub) за день, предшествующему первому дню отчетного периода (если отчет собирается за январь 2018 года, то это 31 декабря 2017 года)
 для рублевых счетов (рублевые счета, это те, у которых код валюты (поле DS.MD_ACCOUNT_D.currency_code равно 810 или 643));
BALANCE_IN_VAL – сумма остатков в рублях за день, предшествующему первому дню отчетного периода для всех счетов, кроме рублевых
BALANCE_IN_TOTAL - сумма остатков в рублях за день, предшествующему первому дню отчетного периода для всех счетов;
TURN_DEB_RUB – сумма дебетовых оборотов в рублях (DM.DM_ACCOUNT_TURNOVER_F.debet_amount_rub) за все дни отчетного периода для рублевых счетов
TURN_DEB_VAL – сумма дебетовых оборотов в рублях за все дни отчетного периода для всех счетов, кроме рублевых;
TURN_DEB_TOTAL – сумма дебетовых оборотов в рублях за все дни отчетного периода для всех счетов
TURN_CRE_RUB – сумма кредитовых оборотов в рублях (DM.DM_ACCOUNT_TURNOVER_F.credit_amount_rub) за все дни отчетного периода для рублевых счетов;
TURN_CRE_VAL – сумма кредитовых оборотов в рублях за все дни отчетного периода для всех счетов, кроме рублевых;
TURN_CRE_TOTAL – сумма кредитовых оборотов в рублях за все дни отчетного периода для всех счетов
BALANCE_OUT_RUB – сумма остатков в рублях (DM.DM_ACCOUNT_BALANCE_F.balance_out_rub) за последний день отчетного периода для рублевых счетов;
BALANCE_OUT_VAL – сумма остатков в рублях за последний день отчетного периода для всех счетов, кроме рублевых;
BALANCE_OUT_TOTAL – сумма остатков в рублях за последний день отчетного периода для всех счетов
В процедуре расчета добавьте логирование на свое усмотрение. 
В качестве таблицы с логами можно использовать таблицу, созданную для предыдущих задач. 
В логах должна быть информация о том какая витрина рассчитывается, дата и время старта и окончания расчета. 
Так же для возможности перезапускать расчет много раз за одну и ту же дату, в процедурах в начале расчета вам необходимо удалять записи за дату расчета. 
Рассчитайте отчет за январь 2018 года. */

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f (i_OnDate date) 
LANGUAGE plpgsql 
AS $$
DECLARE
  date_begin date;
  date_end date;
BEGIN  
date_begin := (date_trunc('month', i_OnDate - interval '1 month')::date);
date_end := ((date_begin + interval '1 month -1 day')::date);

INSERT INTO logs.log_dm (date_import,name_table,status)
	VALUES (NOW(), 'dm.dm_f101_round_f',0);

DELETE FROM dm.dm_f101_round_f WHERE FROM_DATE = date_begin AND TO_DATE = date_end;

INSERT INTO dm.dm_f101_round_f (
	FROM_DATE,
	TO_DATE,
	CHAPTER,
	LEDGER_ACCOUNT,
	CHARACTERISTIC,
	BALANCE_IN_RUB,
	BALANCE_IN_VAL,
	BALANCE_IN_TOTAL,
	TURN_DEB_RUB,
	TURN_DEB_VAL,
	TURN_DEB_TOTAL,
	TURN_CRE_RUB,
	TURN_CRE_VAL,
	TURN_CRE_TOTAL,
	BALANCE_OUT_RUB,
	BALANCE_OUT_VAL,
	BALANCE_OUT_TOTAL)
SELECT 
	date_begin AS FROM_DATE,
	date_end AS TO_DATE,
	dla.chapter AS CHAPTER,
	dla.ledger_account AS LEDGER_ACCOUNT,
	da.char_type AS CHARACTERISTIC,
	SUM(CASE WHEN COALESCE(da.currency_code,'') IN ('810','643') THEN COALESCE(dabf.balance_out_rub,0.0) ELSE 0.0 END) AS BALANCE_IN_RUB,
	SUM(CASE WHEN COALESCE(da.currency_code,'') NOT IN ('810','643') THEN COALESCE(dabf.balance_out_rub,0.0) ELSE 0.0 END) AS BALANCE_IN_VAL,
	SUM(COALESCE(dabf.balance_out_rub,0.0)) AS BALANCE_IN_TOTAL,
	SUM(CASE WHEN COALESCE(da.currency_code,'') IN ('810','643') THEN COALESCE(dat.debet_amount_rub,0.0) ELSE 0.0 END) AS TURN_DEB_RUB,
	SUM(CASE WHEN COALESCE(da.currency_code,'') NOT IN ('810','643') THEN COALESCE(dat.debet_amount_rub,0.0) ELSE 0.0 END) AS TURN_DEB_VAL,
	SUM(COALESCE(dat.debet_amount_rub,0.0)) AS TURN_DEB_TOTAL,
	SUM(CASE WHEN COALESCE(da.currency_code,'') IN ('810','643') THEN COALESCE(dat.credit_amount_rub,0.0) ELSE 0.0 END) AS TURN_CRE_RUB,
	SUM(CASE WHEN COALESCE(da.currency_code,'') NOT IN ('810','643') THEN COALESCE(dat.credit_amount_rub,0.0) ELSE 0.0 END) AS TURN_CRE_VAL,
	SUM(COALESCE(dat.credit_amount_rub,0.0)) AS TURN_CRE_TOTAL,
	SUM(CASE WHEN COALESCE(da.currency_code,'') IN ('810','643') THEN COALESCE(dabl.balance_out_rub,0.0) ELSE 0.0 END) AS BALANCE_OUT_RUB,
	SUM(CASE WHEN COALESCE(da.currency_code,'') NOT IN ('810','643') THEN COALESCE(dabl.balance_out_rub,0.0) ELSE 0.0 END) AS BALANCE_OUT_VAL,
	SUM(COALESCE(dabl.balance_out_rub,0.0)) AS BALANCE_OUT_TOTAL	
FROM ds.md_ledger_account_s dla
INNER JOIN ds.md_account_d da ON dla.ledger_account = LEFT(da.account_number,5)::int
LEFT JOIN dm.dm_account_balance_f dabf ON da.account_rk = dabf.account_rk AND dabf.on_date = date_begin + interval '-1 day'
LEFT JOIN dm.dm_account_balance_f dabl ON da.account_rk = dabl.account_rk AND dabl.on_date = date_end
LEFT JOIN dm.dm_account_turnover_f dat ON da.account_rk = dat.account_rk AND dat.on_date BETWEEN date_begin AND date_end
GROUP BY date_begin, date_end, dla.chapter, dla.ledger_account, da.char_type;

UPDATE dm.dm_f101_round_f 
	SET R_BALANCE_IN_RUB = BALANCE_IN_RUB/1000.0,
		R_BALANCE_IN_VAL = BALANCE_IN_VAL/1000.0,
		R_BALANCE_IN_TOTAL = BALANCE_IN_TOTAL/1000.0,
		R_TURN_DEB_RUB = TURN_DEB_RUB/1000.0,
		R_TURN_DEB_VAL = TURN_DEB_VAL/1000.0,
		R_TURN_DEB_TOTAL = TURN_DEB_TOTAL/1000.0,
		R_TURN_CRE_RUB = TURN_CRE_RUB/1000.0,
		R_TURN_CRE_VAL = TURN_CRE_VAL/1000.0,
		R_TURN_CRE_TOTAL = TURN_CRE_TOTAL/1000.0,
		R_BALANCE_OUT_RUB = BALANCE_OUT_RUB/1000.0,
		R_BALANCE_OUT_VAL = BALANCE_OUT_VAL/1000.0,
		R_BALANCE_OUT_TOTAL = BALANCE_OUT_TOTAL/1000.0;
		
INSERT INTO logs.log_dm (date_import,name_table,status)
	VALUES (NOW(), 'dm.dm_f101_round_f',0);
END;
$$;

CALL dm.fill_f101_round_f ('2018-02-01');
SELECT * FROM dm.dm_f101_round_f;
--TRUNCATE TABLE dm.dm_f101_round_f;
