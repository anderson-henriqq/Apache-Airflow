import psycopg2
from psycopg2 import sql

def create():
   # Conectar ao banco de dados PostgreSQL
    conexao = psycopg2.connect(
        dbname="postgres",
        user="airflow",
        password="airflow",
        host="172.18.0.3",
        port="5432"
    )

    # Criar um cursor para executar comandos SQL
    cursor = conexao.cursor()

    # Comando SQL para criar uma tabela
    criar_tabela = """
    CREATE TABLE IF NOT EXISTS arquivos_csv (
        id SERIAL PRIMARY KEY,
        nome_arquivo VARCHAR(255),
        conteudo BYTEA,  -- Coluna para armazenar o arquivo em formato binário
        data_upload TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """


    # Executar o comando para criar a tabela
    cursor.execute(criar_tabela)
    conexao.commit()
    
    # Fechar a conexão e o cursor
    cursor.close()
    conexao.close()
create ()