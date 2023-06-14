from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from ETL_Manager.Extract.extract_stocks_price_data import Stock_Price_Crawler
from ETL_Manager.Extract.extract_gold_price_data import Gold_Price_Crawler
from ETL_Manager.Extract.extract_interest_rates_data import Interest_Rate_Crawler
from ETL_Manager.Extract.extract_energy_price_data import Energy_Price_Crawler
from ETL_Manager.Extract.get_stock_info import Company_Info_Crawler
from ETL_Manager.group_companies import group_companies
from ETL_Manager.summarize_sjc_price import get_lowest_and_highest_price
from ETL_Manager.load import load_time_dimension, load_main_transactions, load_energy_price, load_interest_rate


spc = Stock_Price_Crawler()
gpc = Gold_Price_Crawler()
irc = Interest_Rate_Crawler()
cic = Company_Info_Crawler()

default_args = {
    'owner': 'phuc',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='etl_process_for_stock_data',
    start_date=datetime(2023, 6, 3),
    schedule_interval='0 22 * * 1-5'
) as dag:
    task1 = PythonOperator(
        task_id='crawl_stocks_price',
        python_callable=spc.crawl_stocks_price
    )

    task2 = PythonOperator(
        task_id='crawl_companies_info',
        python_callable=cic.get_company_info
    )

    task3 = PythonOperator(
        task_id='crawl_gold_price',
        python_callable=gpc.crawl_scj_price
    )

    task4 = PythonOperator(
        task_id='crawl_interest_rates',
        python_callable=irc.crawl_interest_rate
    )

    task5 = PythonOperator(
        task_id='group_companies_and_load_to_companies_dimesional_table',
        python_callable=group_companies
    )

    task6 = PythonOperator(
        task_id='load_gold_dimensional_table',
        python_callable=get_lowest_and_highest_price
    )

    task7 = PythonOperator(
        task_id='load_time_dimensional_table',
        python_callable=load_time_dimension
    )

    task8 = PythonOperator(
        task_id='load_interest_rate_table',
        python_callable=load_interest_rate
    )

    task9 = PythonOperator(
        task_id='load_ennergy_price',
        python_callable=load_energy_price
    )

    task10 = PythonOperator(
        task_id='load_main_trasactions',
        python_callable=load_main_transactions
    )

    task2 >> task5
    task3 >> task6
    task4 >> task8
    [task1, task5, task6, task7, task8, task9] >> task10



    