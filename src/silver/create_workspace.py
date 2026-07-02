from func.get_logging import get_logging
from func.config import get_spark

logging = get_logging("silver/create_workspace")


spark = get_spark()



def main():
    try:
        logging.info("criando ambiente")
        spark.sql("create catalog if not exists demo_dab")
        spark.sql("create schema if not exists demo_dab.silver")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS demo_dab.silver.pessoas (
                client_id int,
                nome STRING,
                idade INT,
                data_carga TIMESTAMP,
                data_update TIMESTAMP
            )
        """)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS demo_dab.silver.itens (
                item_id int,
                nome_item STRING,
                data_carga TIMESTAMP,
                data_update TIMESTAMP
            )
        """)        
        logging.info("ambiente criado com sucesso")
    except Exception as e:
        logging.error(f"{e}")
        raise