import psycopg2

def insert_csv():  
    conexao = psycopg2.connect(
        dbname="postgres",
        user="airflow",
        password="airflow",
        host="172.18.0.3",
        port="5432"
    )

    cursor = conexao.cursor()

    file_path = '/home/andersonhenriq/Downloads/Datasets/archive/EV_Population.csv'

    with open(file_path, 'rb') as arquivo_csv:
        conteudo = arquivo_csv.read()
        
        inserir_arquivo = """
        INSERT INTO arquivos_csv (nome_arquivo, conteudo)
        VALUES (%s, %s);
        """
        
        nome_arquivo = file_path.split('/')[-1]
        print(nome_arquivo)
        cursor.execute(inserir_arquivo, (nome_arquivo, conteudo))
        conexao.commit()

    cursor.close()
    conexao.close()

# Exemplo de chamada da função
insert_csv()

