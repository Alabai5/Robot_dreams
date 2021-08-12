from datetime import date
import pyspark
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def load_tables_dshop_db_from_bronze_to_silver_spark(table):

    last_partition_date = str(date.today())

    logging.info(f"Create spark session")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared_folder/rotot_dreams/postgresql-42.2.23.jar') \
        .master('local') \
        .appName("FromDshopBronzeToSilver") \
        .getOrCreate()

    logging.info(f"Writing table {table} from Bronze to Silver")
    logging.info(f"Create DataFrame based on the table {table}")
    bronze_df = spark.read.load(f"/DataLake/bronze/dshop/{table}/{last_partition_date}/{table}.csv"
                                       , header="true"
                                       , inferSchema="true"
                                       , format="csv"
                                       )
    logging.info(f"Finished create DataFrame based on the table {table}")
    logging.info(f"Delete duplicate keys from DataFrame {table}")
    clear_on_the_dup_df = bronze_df.dropDuplicates()
    logging.info(f"Writing {table} to silver DataLake")
    clear_on_the_dup_df.write \
        .parquet(f"/DataLake/silver/dshop/{table}", mode='overwrite')
    logging.info(f"Success!!!")

