from func.get_logging import get_logging
from func.config import get_spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
logging = get_logging("silver/transform_pessoas")

spark = get_spark()

def main():
    logging.info("iniciando processamento de pessoas")
    df = spark.read.table("demo_dab.bronze.pessoas")
    df_bronze = df.select(
        F.col("client_id"),
        F.upper(F.col("nome")).alias("nome"),
        F.col("idade"),
        F.col("data_carga"),

    )

    partition_by = Window.partitionBy("client_id").orderBy(F.col("data_carga").desc())
    df_silver = (
        df_bronze.withColumn("dp",F.row_number().over(partition_by))
        .filter(F.col("dp") == 1)
        .drop(F.col("dp"))
    )

    target_table = "demo_dab.silver.pessoas"
    date = datetime.now()
    target = DeltaTable.forName(spark,target_table)
    logging.info("iniciando merge para pessoas")

    (
        target.alias("tg")
        .merge(
            source = df_silver.alias("sc"),
            condition= f"tg.client_id = sc.client_id"
        )
        .whenMatchedUpdate(
            set={
                "tg.nome": "sc.nome",
                "tg.idade": "sc.idade",
                "tg.data_update": f"'{date}'"
            }
        )
        .whenNotMatchedInsert(
            values={
                "tg.client_id": "sc.client_id",
                "tg.nome": "sc.nome",
                "tg.idade": "sc.idade",
                "tg.data_carga": "sc.data_carga"
            }
        )
        .execute()
    )
    logging.info("merge finalizado com sucesso")