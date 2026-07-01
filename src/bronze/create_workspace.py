from func.get_logging import get_logging
from func.config import get_spark

logging = get_logging("bronze/create_workspace")


spark = get_spark()



def main():
    try:
        logging.info("teste cicd")
        logging.info("criando ambiente")
        spark.sql("create catalog if not exists demo_dab")
        spark.sql("create schema if not exists demo_dab.bronze")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS demo_dab.bronze.pessoas (
                client_id int,
                nome STRING,
                idade INT
            )
        """)
        logging.info("ambiente criado com sucesso")
    except Exception as e:
        logging.error(f"{e}")
        raise