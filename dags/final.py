from airflow.models import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import csv
import psycopg2
import requests
from datetime import datetime
import pandas as pd

# Define default arguments for the DAG
default_args = {
    'start_date': datetime(2024, 2, 4, 15, 6, 0) - timedelta(hours=7) # Start date for the DAG
}

with DAG('final',  # Name of the DAG
         schedule_interval='@daily',  # Schedule interval
         default_args=default_args,  # Default arguments
         catchup=False) as dag:  # No catchup
        # test

    def process_user():
        processed_user_list = []
        url = 'https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv'
        response = requests.get(url)
        if response.status_code == 200:
            lines = response.text.split('\n')
            csvreader = csv.DictReader(lines)
            for row in csvreader:
                processed_user_list.append(row)
        return processed_user_list

    processing_user = PythonOperator(
        task_id="processing_user",
        python_callable=process_user,
        dag=dag
    )
    creating_table = PostgresOperator(
        task_id='creating_table',  # Task ID
        postgres_conn_id='airflow_postgres',  # Connection ID
        sql="""
            CREATE TABLE IF NOT EXISTS churn_modelling 
            (RowNumber INTEGER PRIMARY KEY, 
            CustomerId INTEGER, 
            Surname VARCHAR(50), 
            CreditScore INTEGER, 
            Geography VARCHAR(50), 
            Gender VARCHAR(20), 
            Age INTEGER, 
            Tenure INTEGER, 
            Balance FLOAT, 
            NumOfProducts INTEGER, 
            HasCrCard INTEGER, 
            IsActiveMember INTEGER, 
            EstimatedSalary FLOAT, 
            Exited INTEGER
            )
            """
    )

    def store_user(**kwargs):
        task_instance = kwargs['task_instance']
        processed_data_list = task_instance.xcom_pull(task_ids='processing_user')

        with psycopg2.connect(
            database="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        ) as conn:
            with conn.cursor() as cur:
                for user_data in processed_data_list:
                    sql_query = """
                        INSERT INTO churn_modelling (
                            RowNumber, CustomerId, Surname, CreditScore, Geography, Gender,
                            Age, Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember,
                            EstimatedSalary, Exited
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    sql_data = (
                        user_data['RowNumber'],
                        user_data['CustomerId'],
                        user_data['Surname'],
                        user_data['CreditScore'],
                        user_data['Geography'],
                        user_data['Gender'],
                        user_data['Age'],
                        user_data['Tenure'],
                        user_data['Balance'],
                        user_data['NumOfProducts'],
                        user_data['HasCrCard'],
                        user_data['IsActiveMember'],
                        user_data['EstimatedSalary'],
                        user_data['Exited']
                    )
                    cur.execute(sql_query, sql_data)
                    # Commit the changes for each iteration
                    conn.commit()

    storing_user = PythonOperator(
        task_id="storing_user",
        python_callable=store_user,
        provide_context=True,
        dag=dag
    )

    # Set task dependencies
    creating_table >> processing_user >> storing_user
