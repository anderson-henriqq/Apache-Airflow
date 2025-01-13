from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Definindo argumentos padrão
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Criando o DAG
with DAG(
    'create_table_in_postgres',
    default_args=default_args,
    description='DAG para testar conexão com o PostgreSQL e criar uma tabela',
    schedule_interval=None,  # Este DAG será executado manualmente
    start_date=datetime(2025, 1, 12),
) as dag:

    # Tarefa para criar uma tabela no banco de dados
    create_table = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='db_teste',  # O Conn ID que você configurou no Airflow
        sql="""
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        """,  # SQL para criar a tabela
    )

    # Executando a tarefa
    create_table
