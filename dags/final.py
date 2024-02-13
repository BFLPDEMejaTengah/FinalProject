from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import csv
import psycopg2
import requests
import pandas as pd

# Define default arguments for the DAG
default_args = {
    'start_date': datetime(2024, 2, 4, 15, 6, 0) - timedelta(hours=7) # Start date for the DAG
}

#FUNCTION TO EXTRACT TABLE AND CONVER TO CSV
def table_to_csv(table_name, csv_file_path, db_connection_params):

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(**db_connection_params)

        # Read SQL query into a DataFrame
        df = pd.read_sql_query(f"SELECT * FROM {table_name};", conn)

        # Export DataFrame to CSV
        df.to_csv(csv_file_path, index=False)

        print(f"Table '{table_name}' successfully exported to CSV: {csv_file_path}")

    except (Exception, psycopg2.Error) as error:
        print("Error exporting table to CSV:", error)

    finally:
        # Close database connection
        if conn:
            conn.close()

with DAG('final',  # Name of the DAG
         schedule_interval='@daily',  # Schedule interval
         default_args=default_args,  # Default arguments
         catchup=False) as dag:  # No catchup

    def getting_data():
        processed_user_list = []
        url = 'https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv'
        response = requests.get(url)
        if response.status_code == 200:
            lines = response.text.split('\n')
            csvreader = csv.DictReader(lines)
            for row in csvreader:
                processed_user_list.append(row)
        return processed_user_list

    getting_data = PythonOperator(
        task_id="getting_data",
        python_callable=getting_data,
        dag=dag
    )

    creating_churn_modelling_table = PostgresOperator(
        task_id='creating_churn_modelling_table',  # Task ID
        postgres_conn_id='airflow_postgres',  # Connection ID
        sql="""
            DROP TABLE IF EXISTS churn_modelling;
            CREATE TABLE IF NOT EXISTS churn_modelling (
                RowNumber INTEGER PRIMARY KEY, 
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
            );
            """
    )

    # Define the PostgresOperator task
    creating_churn_modelling_creditscore_table = PostgresOperator(
        task_id='creating_churn_modelling_creditscore_table',  # Task ID
        postgres_conn_id='airflow_postgres',  # Connection ID
        sql='''
            DROP TABLE IF EXISTS churn_modelling_creditscore;
            CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (
                geography VARCHAR(50), 
                gender VARCHAR(20), 
                avg_credit_score FLOAT, 
                total_exited INTEGER
            );
        '''
    )

    # Define the PostgresOperator task
    creating_churn_modelling_exited_age_correlation_table = PostgresOperator(
        task_id='creating_churn_modelling_exited_age_correlation_table',  # Task ID
        postgres_conn_id='airflow_postgres',  # Connection ID
        sql='''
            DROP TABLE IF EXISTS churn_modelling_exited_age_correlation;
            CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (
                geography VARCHAR(50), 
                gender VARCHAR(20), 
                exited INTEGER, 
                avg_age FLOAT, 
                avg_salary FLOAT,
                number_of_exited_or_not INTEGER
            );
        '''
    )

    # Define the PostgresOperator task
    creating_churn_modelling_exited_salary_correlation_table = PostgresOperator(
        task_id='creating_churn_modelling_exited_salary_correlation_table',  # Task ID
        postgres_conn_id='airflow_postgres',  # Connection ID
        sql='''
            DROP TABLE IF EXISTS churn_modelling_exited_salary_correlation;
            CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation  (
                exited INTEGER, 
                is_greater INTEGER, 
                correlation INTEGER
            );
        '''
    )

    def store_user(**kwargs):
        task_instance = kwargs['task_instance']
        processed_data_list = task_instance.xcom_pull(task_ids='getting_data')

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

    ## TASK TO EXPORT TO CSV
    export_to_csv_task = PythonOperator(
        task_id="export_to_csv_task",
        python_callable=table_to_csv,
        op_kwargs={
            'table_name': 'churn_modelling',
            'csv_file_path': '/opt/airflow/csv/churn_modelling2.csv',
            'db_connection_params': {
                    'database': 'airflow',
                    'user': 'airflow',
                    'password': 'airflow',
                    'host': 'postgres',
                    'port': '5432'
            }
        },
        dag=dag
    )

def create_base_df(cur):
    """
    Base dataframe of churn_modelling table
    """
    cur.execute("""SELECT * FROM churn_modelling""")
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)
    df.drop('rownumber', axis=1, inplace=True)
    index_to_be_null = np.random.randint(10000, size=30)
    df.loc[index_to_be_null, ['balance', 'creditscore', 'geography']] = np.nan
    most_occured_country = df['geography'].value_counts().index[0]
    df['geography'].fillna(value=most_occured_country, inplace=True)
    avg_balance = df['balance'].mean()
    df['balance'].fillna(value=avg_balance, inplace=True)
    median_creditscore = df['creditscore'].median()
    df['creditscore'].fillna(value=median_creditscore, inplace=True)
    return df

def create_creditscore_df(df):
    df_creditscore = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean', 'exited':'sum'})
    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
    df_creditscore.reset_index(inplace=True)
    df_creditscore.sort_values('avg_credit_score', inplace=True)
    return df_creditscore

def create_exited_age_correlation(df):
    df_exited_age_correlation = df.groupby(['geography', 'gender', 'exited']).agg({
        'age': 'mean',
        'estimatedsalary': 'mean',
        'exited': 'count'
    }).rename(columns={
        'age': 'avg_age',
        'estimatedsalary': 'avg_salary',
        'exited': 'number_of_exited_or_not'
    }).reset_index().sort_values('number_of_exited_or_not')
    return df_exited_age_correlation

def create_exited_salary_correlation(df):
    df_salary = df[['geography', 'gender', 'exited', 'estimatedsalary']].groupby(['geography', 'gender']).agg({'estimatedsalary':'mean'}).sort_values('estimatedsalary')
    df_salary.reset_index(inplace=True)
    min_salary = round(df_salary['estimatedsalary'].min(), 0)
    df['is_greater'] = df['estimatedsalary'].apply(lambda x: 1 if x > min_salary else 0)
    df_exited_salary_correlation = pd.DataFrame({
        'exited': df['exited'],
        'is_greater': df['estimatedsalary'] > df['estimatedsalary'].min(),
        'correlation': np.where(df['exited'] == (df['estimatedsalary'] > df['estimatedsalary'].min()), 1, 0)
    })
    return df_exited_salary_correlation

def create_new_tables_in_postgres():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT, number_of_exited_or_not INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")
        logging.info("3 tables created successfully in Postgres server")
    except Exception as e:
        traceback.print_exc()
        logging.error(f'Tables cannot be created due to: {e}')

def insert_creditscore_table(df_creditscore):
    query = "INSERT INTO churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES (%s,%s,%s,%s)"
    row_count = 0
    for _, row in df_creditscore.iterrows():
        values = (row['geography'], row['gender'], row['avg_credit_score'], row['total_exited'])
        cur.execute(query, values)
        row_count += 1
    logging.info(f"{row_count} rows inserted into table churn_modelling_creditscore")

def insert_exited_age_correlation_table(df_exited_age_correlation):
    query = """INSERT INTO churn_modelling_exited_age_correlation (Geography, Gender, exited, avg_age, avg_salary, number_of_exited_or_not) VALUES (%s,%s,%s,%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_age_correlation.iterrows():
        values = (row['geography'], row['gender'], row['exited'], row['avg_age'], row['avg_salary'], row['number_of_exited_or_not'])
        cur.execute(query, values)
        row_count += 1
    logging.info(f"{row_count} rows inserted into table churn_modelling_exited_age_correlation")

def insert_exited_salary_correlation_table(df_exited_salary_correlation):
    query = """INSERT INTO churn_modelling_exited_salary_correlation (exited, is_greater, correlation) VALUES (%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_salary_correlation.iterrows():
        values = (int(row['exited']), int(row['is_greater']), int(row['correlation']))
        cur.execute(query, values)
        row_count += 1
    logging.info(f"{row_count} rows inserted into table churn_modelling_exited_salary_correlation")


    # Set task dependencies
    creating_churn_modelling_table >> getting_data >> storing_user >> export_to_csv_task
    creating_churn_modelling_creditscore_table >> getting_data
    creating_churn_modelling_exited_age_correlation_table >> getting_data
    creating_churn_modelling_exited_salary_correlation_table >> getting_data