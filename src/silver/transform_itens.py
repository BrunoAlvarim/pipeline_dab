from func.get_logging import get_logging
from func.config import get_spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
logging = get_logging("silver/transform_itens")

spark = get_spark()

def main():
    logging.info("iniciando processamento de itens")
    df = spark.read.table("demo_dab.bronze.itens")
    df_bronze = df.select(
        F.col("item_id"),
        F.upper(F.col("nome_item")).alias("nome_item"),
        F.col("data_carga"),

    )

    partition_by = Window.partitionBy("item_id").orderBy(F.col("data_carga").desc())
    df_silver = (
        df_bronze.withColumn("dp",F.row_number().over(partition_by))
        .filter(F.col("dp") == 1)
        .drop(F.col("dp"))
    )

    target_table = "demo_dab.silver.itens"
    date = datetime.now()
    target = DeltaTable.forName(spark,target_table)
    logging.info("iniciando merge para itens")

    (
        target.alias("tg")
        .merge(
            source = df_silver.alias("sc"),
            condition= f"tg.item_id = sc.item_id"
        )
        .whenMatchedUpdate(
            set={
                "tg.nome_item": "sc.nome_item",
                "tg.data_update": f"'{date}'"
            }
        )
        .whenNotMatchedInsert(
            values={
                "tg.item_id": "sc.item_id",
                "tg.nome_item": "sc.nome_item",
                "tg.data_carga": "sc.data_carga"
            }
        )
        .execute()
    )
    logging.info("merge finalizado com sucesso")