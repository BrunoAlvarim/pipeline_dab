from func.get_logging import get_logging
from func.config import get_spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType
import random
from datetime import datetime
logging = get_logging("bronze/insert_itens")

spark = get_spark()

# Sample item names for data generation
SAMPLE_ITEMS = [
    "Notebook", "Mouse", "Teclado", "Monitor",
    "Cadeira", "Mesa", "Impressora", "Headset"
]


def main():
    """
    Insert a single random item record into the table.
    item_id: random int from 1 to 10
    nome_item: random name from list
    """
    try:
        item_id = random.randint(1, 10)
        nome_item = random.choice(SAMPLE_ITEMS)
        data_carga = datetime.now()
        
        schema = StructType([
            StructField("item_id", IntegerType(), False),
            StructField("nome_item", StringType(), False),
            StructField("data_carga", TimestampType(), False)
        ])

        data = [(item_id, nome_item, data_carga)]
        df = spark.createDataFrame(data, schema=schema)

        logging.info(f"Inserting record: item_id={item_id}, nome_item={nome_item}")
        df.show()

        df.write.format("delta").mode("append").saveAsTable("demo_dab.bronze.itens")

        logging.info(f"Successfully inserted 1 record into demo_dab.bronze.itens")

    except Exception as e:
        logging.error(f"insert_random_item | Error: {e}")
        raise