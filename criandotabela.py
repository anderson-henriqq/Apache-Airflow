import psycopg2
from psycopg2 import sql

def create():
   # Conectar ao banco de dados PostgreSQL
    conexao = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="172.17.0.2",
        port="5432"
    )

    # Criar um cursor para executar comandos SQL
    cursor = conexao.cursor()

    # Comando SQL para criar uma tabela
    criar_tabela = """
    CREATE TABLE IF NOT EXISTS produtos_transformados (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100),
        preco NUMERIC(10, 2),
        preco_com_desconto NUMERIC(10, 2)
    );
    """


    # Executar o comando para criar a tabela
    cursor.execute(criar_tabela)
    conexao.commit()
    
    # Fechar a conexão e o cursor
    cursor.close()
    conexao.close()

create()
