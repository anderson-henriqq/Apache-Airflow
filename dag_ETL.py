from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'etl_data_limpeza',
    default_args=default_args,
    description='Pipeline ETL com limpeza de dados usando Airflow',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 14),
    catchup=False
) as dag:
    
    def read_csv_data(**kwargs):
        csv_path = '/home/andersonhenriq/Downloads/Datasets/archive/EV_Population.csv'
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} não encontrado!")
        
        df = pd.read_csv(csv_path)
        df.to_csv('/tmp/raw_data.csv', index=False)  # Salva como um arquivo temporário
        print('Leitura do CSV concluída!')

    def clean_data():
        raw_data_path = '/tmp/raw_data.csv'
        if not os.path.exists(raw_data_path):
            raise FileNotFoundError(f"{raw_data_path} não encontrado!")
        
        df = pd.read_csv(raw_data_path)
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        
        df.to_csv('/tmp/cleaned_data.csv', index=False)
        print('Limpeza concluída!')

    def load_data_to_db():
        cleaned_data_path = '/tmp/cleaned_data.csv'
        if not os.path.exists(cleaned_data_path):
            raise FileNotFoundError(f"{cleaned_data_path} não encontrado!")

        df = pd.read_csv(cleaned_data_path)
        postgres_hook = PostgresHook(postgres_conn_id='conn_postgres')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO electric_vehicles (state, model_year, make, electric_vehicle_type, electric_range, base_msrp, legislative_district, cafv_eligibility_simple)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    row['State'],  # Substitua pelo nome real da coluna no DataFrame
                    row['Model Year'],  # Substitua pelo nome real da coluna no DataFrame
                    row['Make'],   # Substitua pelo nome real da coluna no DataFrame
                    row['Electric Vehicle Type'],  # Substitua pelo nome real da coluna no DataFrame
                    row['Electric Range'],  # Substitua pelo nome real da coluna no DataFrame
                    row['Base MSRP'],  # Substitua pelo nome real da coluna no DataFrame
                    row['Legislative District'],  # Substitua pelo nome real da coluna no DataFrame
                    row['CAFV Eligibility Simple']  # Substitua pelo nome real da coluna no DataFrame
                )
            )
        conn.commit()
        cursor.close()
        print('Dados carregados no banco de dados com sucesso!')


    read_csv_task = PythonOperator(
        task_id='read_csv_data',
        python_callable=read_csv_data,
        provide_context=True
    )

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db
    )

read_csv_task >> clean_data_task >> load_data_task