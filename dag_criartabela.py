from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from ..scripts.criandotabela import create



with DAG(
    dag_id="dag_criartabela",
    start_date= datetime(2024,1,3),
    schedule_interval= None,
    catchup= False
) as dag:
    
    criar_tabela=PythonOperator(
        task_id='criarTabela',
        python_callable=create, #chama a função importada
        dag= dag)

criar_tabela