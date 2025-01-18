CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
	chapter				CHAR(1),
	chapter_name			VARCHAR(16),
	section_number			INT,
	section_name			VARCHAR(22),
	subsection_name			VARCHAR(21),
	ledger1_account			INT,
	ledger1_account_name		VARCHAR(47),
	ledger_account			INT not null,
	ledger_account_name		VARCHAR(153),
	characteristic			CHAR(1),
	is_resident			INT,
	is_reserve			INT,
	is_reserved			INT,
	is_loan				INT,
	is_reserved_assets		INT,
	is_overdue			INT,
	is_interest			INT,
	pair_account			VARCHAR(5),
	start_date			DATE not null,
	end_date			DATE,
	is_rub_only			INT,
	min_term			VARCHAR(1),
	min_term_measure		VARCHAR(1),
	max_term			VARCHAR(1),
	max_term_measure		VARCHAR(1),
	ledger_acc_full_name_translit	VARCHAR(1),
	is_revaluation			VARCHAR(1),
	is_correct			VARCHAR(1),
    	PRIMARY KEY			(ledger_account,start_date)
);

MERGE INTO ds.md_ledger_account_s AS d 
USING (SELECT 
		"CHAPTER" AS chapter,
		"CHAPTER_NAME" AS chapter_name,
		"SECTION_NUMBER" AS section_number,
		"SECTION_NAME" AS section_name,
		"SUBSECTION_NAME" AS subsection_name,
		"LEDGER1_ACCOUNT" AS ledger1_account,
		"LEDGER1_ACCOUNT_NAME" AS ledger1_account_name,
		"LEDGER_ACCOUNT" AS ledger_account,
		"LEDGER_ACCOUNT_NAME" AS ledger_account_name,
		"CHARACTERISTIC" AS characteristic,
		to_date("START_DATE",'YYYY-MM-DD') AS start_date,
		to_date("END_DATE",'YYYY-MM-DD') AS end_date
		FROM stage.md_ledger_account_s
 		WHERE "LEDGER_ACCOUNT" IS NOT NULL
		 	AND "START_DATE" IS NOT NULL) AS s
ON d.ledger_account = s.ledger_account AND d.start_date = s.start_date
WHEN MATCHED THEN UPDATE 
	SET 
	chapter = s.chapter,
	chapter_name = s.chapter_name,
	section_number = s.section_number,
	section_name = s.section_name,
	subsection_name = s.subsection_name,
	ledger1_account = s.ledger1_account,
	ledger1_account_name = s.ledger1_account_name,
	ledger_account_name = s.ledger_account_name,
	characteristic = s.characteristic,
	end_date = s.end_date
WHEN NOT MATCHED THEN 
	INSERT (
	chapter,
	chapter_name,
	section_number,
	section_name,
	subsection_name,
	ledger1_account,
	ledger1_account_name,
	ledger_account,
	ledger_account_name,
	characteristic,
	start_date,
	end_date)
	VALUES (
	s.chapter,
	s.chapter_name,
	s.section_number,
	s.section_name,
	s.subsection_name,
	s.ledger1_account,
	s.ledger1_account_name,
	s.ledger_account,
	s.ledger_account_name,
	s.characteristic,
	s.start_date,
	s.end_date);
