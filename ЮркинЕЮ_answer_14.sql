INSERT INTO logs.log_dm (date_import,name_table,status) VALUES (NOW(), 'dm_f101_round_f.csv',0);

COPY dm.dm_f101_round_f TO 'C:\ENY\NeoStudy\airflow\files\f101\dm_f101_round_f.csv' DELIMITER ';' CSV HEADER;

INSERT INTO logs.log_dm (date_import,name_table,status) VALUES (NOW(), 'dm_f101_round_f.csv',1);


CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 (
FROM_DATE 			      DATE,
TO_DATE				        DATE,
CHAPTER				        CHAR(1),
LEDGER_ACCOUNT		    CHAR(5),
CHARACTERISTIC		    CHAR(1),
BALANCE_IN_RUB		    DECIMAL(23,8),
R_BALANCE_IN_RUB	    DECIMAL(23,8),
BALANCE_IN_VAL		    DECIMAL(23,8),
R_BALANCE_IN_VAL	    DECIMAL(23,8),
BALANCE_IN_TOTAL	    DECIMAL(23,8),
R_BALANCE_IN_TOTAL	  DECIMAL(23,8),
TURN_DEB_RUB		      DECIMAL(23,8),
R_TURN_DEB_RUB		    DECIMAL(23,8),
TURN_DEB_VAL		      DECIMAL(23,8),
R_TURN_DEB_VAL		    DECIMAL(23,8),
TURN_DEB_TOTAL		    DECIMAL(23,8),
R_TURN_DEB_TOTAL	    DECIMAL(23,8),
TURN_CRE_RUB		      DECIMAL(23,8),
R_TURN_CRE_RUB		    DECIMAL(23,8),
TURN_CRE_VAL		      DECIMAL(23,8),
R_TURN_CRE_VAL		    DECIMAL(23,8),
TURN_CRE_TOTAL		    DECIMAL(23,8),
R_TURN_CRE_TOTAL	    DECIMAL(23,8),
BALANCE_OUT_RUB		    DECIMAL(23,8),
R_BALANCE_OUT_RUB	    DECIMAL(23,8),
BALANCE_OUT_VAL		    DECIMAL(23,8),
R_BALANCE_OUT_VAL	    DECIMAL(23,8),
BALANCE_OUT_TOTAL	    DECIMAL(23,8),
R_BALANCE_OUT_TOTAL	  DECIMAL(23,8));

INSERT INTO logs.log_dm (date_import,name_table,status) VALUES (NOW(), 'dm.dm_f101_round_f_v2',0);

TRUNCATE TABLE dm.dm_f101_round_f_v2;

COPY dm.dm_f101_round_f_v2 FROM 'C:\ENY\NeoStudy\airflow\files\f101\dm_f101_round_f.csv' DELIMITER ';' CSV HEADER;

INSERT INTO logs.log_dm (date_import,name_table,status) VALUES (NOW(), 'dm.dm_f101_round_f_v2',1);
