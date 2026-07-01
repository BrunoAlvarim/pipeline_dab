from func.get_logging import get_logging
from func.config import get_spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import random

logging = get_logging("bronze/insert_pessoas")

spark = get_spark()

# Sample names for data generation
SAMPLE_NAMES = [
    "João Silva", "Maria Santos", "Pedro Oliveira", "Ana Costa",
    "Carlos Ferreira", "Juliana Martins", "Lucas Almeida", "Fernanda Rocha"
]


def main():
    """
    Insert a single random pessoa record into the table.
    client_id: random int from 1 to 10
    nome: random name from list
    idade: random int from 18 to 80
    """
    try:
        client_id = random.randint(1, 10)
        nome = random.choice(SAMPLE_NAMES)
        idade = random.randint(18, 80)

        schema = StructType([
            StructField("client_id", IntegerType(), False),
            StructField("nome", StringType(), False),
            StructField("idade", IntegerType(), False)
        ])

        data = [(client_id, nome, idade)]
        df = spark.createDataFrame(data, schema=schema)

        logging.info(f"Inserting record: client_id={client_id}, nome={nome}, idade={idade}")
        df.show()

        df.write.format("delta").mode("append").saveAsTable("demo_dab.bronze.pessoas")

        logging.info(f"Successfully inserted 1 record into demo_dab.bronze.pessoas")

    except Exception as e:
        logging.error(f"insert_random_pessoa | Error: {e}")
        raise