CREATE TABLE IF NOT EXISTS ds.ft_posting_f (
	oper_date		DATE not null,
	credit_account_rk	INT not null,
	debet_account_rk	INT not null,
	credit_amount		NUMERIC(19,2),
	debet_amount		NUMERIC(19,2)
);

TRUNCATE TABLE ds.ft_posting_f;

INSERT INTO ds.ft_posting_f (
	oper_date,
	credit_account_rk,
	debet_account_rk,
	credit_amount,
	debet_amount
)
SELECT 
	to_date("OPER_DATE", 'DD-MM-YYYY') AS oper_date,
	"CREDIT_ACCOUNT_RK" AS credit_account_rk,
	"DEBET_ACCOUNT_RK" AS debet_account_rk,
	"CREDIT_AMOUNT" AS credit_amount,
	"DEBET_AMOUNT" AS debet_amount
FROM stage.ft_posting_f
WHERE "OPER_DATE" IS NOT NULL
	AND"CREDIT_ACCOUNT_RK" IS NOT NULL
	AND "DEBET_ACCOUNT_RK" IS NOT NULL;
