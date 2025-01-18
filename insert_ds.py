from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable

import pandas
from datetime import datetime
import time

PATH = Variable.get("my_path")
conf.set("core","template_searchpath",PATH)

def insert_data(table_name):
    df = pandas.read_csv(PATH + f"{table_name}.csv",delimiter=";")
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name,engine,schema="stage",if_exists="replace",index=False)
default_args = {
    "owner" : "postgres",
    "start_date" : datetime(2025, 1, 16),
     "retries" : 2
}

def insert_data2(table_name):
    df = pandas.read_csv(PATH + f"md_currency_d.csv",delimiter=";",encoding="cp1252")
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name,engine,schema="stage",if_exists="replace",index=False)
default_args = {
    "owner" : "postgres",
    "start_date" : datetime(2025, 1, 16),
     "retries" : 2
}

with DAG(
    "insert_ds",
    default_args=default_args,
    description="Загрузка данных в stage",
    catchup=False,
    template_searchpath = [PATH],
    schedule="0 0 * * *"
) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    pause_five_sec = PythonOperator(
        task_id="pause_five_sec", 
        python_callable=lambda: time.sleep(5)
    )

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="postgres-db",
        sql="CREATE SCHEMA IF NOT EXISTS ds; CREATE SCHEMA IF NOT EXISTS logs; CREATE SCHEMA IF NOT EXISTS stage;"
    )

    logs_import_begin = SQLExecuteQueryOperator(
        task_id="logs_import_begin",
        conn_id="postgres-db",
        sql="CREATE TABLE IF NOT EXISTS logs.log_import (type bit, dt timestamp); INSERT INTO logs.log_import (type, dt) VALUES (cast(0 as bit),NOW());"    
    )    

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name":"ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name":"ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name":"md_account_d"}
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data2,
        op_kwargs={"table_name":"md_currency_d"}
    ) 

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name":"md_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name":"md_ledger_account_s"}         
    )  

    split = DummyOperator(
        task_id="split"
    )

    merge_ds_ft_balance_f = SQLExecuteQueryOperator(
        task_id="merge_ds_ft_balance_f",
        conn_id="postgres-db",
        sql="sql/ds_ft_balance_f.sql"
    )    

    insert_ds_ft_posting_f = SQLExecuteQueryOperator(
        task_id="insert_ds_ft_posting_f",
        conn_id="postgres-db",
        sql="sql/ds_ft_posting_f.sql"
    ) 

    merge_ds_md_account_d = SQLExecuteQueryOperator(
        task_id="merge_ds_md_account_d",
        conn_id="postgres-db",
        sql="sql/ds_md_account_d.sql"
    )

    merge_ds_md_currency_d = SQLExecuteQueryOperator(
        task_id="merge_ds_md_currency_d",
        conn_id="postgres-db",
        sql="sql/ds_md_currency_d.sql"
    )

    merge_ds_md_exchange_rate_d = SQLExecuteQueryOperator(
        task_id="merge_ds_md_exchange_rate_d",
        conn_id="postgres-db",
        sql="sql/ds_md_exchange_rate_d.sql"
    )

    merge_ds_md_ledger_account_s = SQLExecuteQueryOperator(
        task_id="merge_ds_md_ledger_account_s",
        conn_id="postgres-db",
        sql="sql/ds_md_ledger_account_s.sql"
    ) 

    logs_import_end = SQLExecuteQueryOperator(
        task_id="logs_import_end",
        conn_id="postgres-db",
        sql="INSERT INTO logs.log_import (type, dt) VALUES (cast(1 as bit),NOW());"    
    )    

    end = DummyOperator(
        task_id = "end"
    )

    (
        start 
        >> create_schema
        >> logs_import_begin
        >> pause_five_sec
        >> [ft_balance_f,ft_posting_f,md_account_d,md_currency_d,md_exchange_rate_d,md_ledger_account_s]
        >> split
        >> [merge_ds_ft_balance_f,insert_ds_ft_posting_f,merge_ds_md_account_d,merge_ds_md_currency_d,merge_ds_md_exchange_rate_d,merge_ds_md_ledger_account_s]
        >> logs_import_end
        >> end
    )
