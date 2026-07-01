from databricks.connect import DatabricksSession
from dotenv import load_dotenv
from .get_logging import get_logging 
import os
load_dotenv()

logging = get_logging("config")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST","")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN","")
IS_DEV = os.getenv("IS_DEV","FALSE")





from pyspark.sql import SparkSession

def get_spark():
    try:
        if IS_DEV == "TRUE":
            if not DATABRICKS_HOST:
                raise ValueError("Variavel DATABRICKS_HOST vazia")
            if not DATABRICKS_TOKEN:
                raise ValueError("Variavel DATABRICKS_TOKEN vazia")

            spark = (
                DatabricksSession.builder
                .serverless()
                .host(DATABRICKS_HOST)
                .token(DATABRICKS_TOKEN)
                .getOrCreate()
            )
            return spark

        elif IS_DEV == "FALSE":
            spark = SparkSession.builder.getOrCreate()
            return spark

    except Exception as e:
        logging.error(f"get_spark | {e}")