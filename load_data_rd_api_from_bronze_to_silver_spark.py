from datetime import date
import pyspark
import logging

from pyspark.sql import SparkSession

def load_data_rd_api_from_bronze_to_silver_spark():
    last_partition_date = str(date.today())

    logging.info(f"Create spark session")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared_folder/rotot_dreams/postgresql-42.2.23.jar') \
        .master('local') \
        .appName("FromAPIBronzeToSilver") \
        .getOrCreate()
    logging.info(f"Writing data {last_partition_date} from Bronze to Silver")
    logging.info(f"Create DataFrame based on the api folder {last_partition_date}")

    bronze_df = spark.read.load(f"/DataLake/bronze/RobotDreamsAPI/{last_partition_date}/Product-{last_partition_date}.json"
                                , header="true"
                                , inferSchema="true"
                                , format="json"

                                )
    logging.info(f"Finished create DataFrame based on the api {last_partition_date}")
    logging.info(f"Delete duplicate keys from DataFrame api {last_partition_date}")
    clear_on_the_dup_product_df = bronze_df.dropDuplicates()
    logging.info(f"Writing data to {last_partition_date} to silver DataLake")
    clear_on_the_dup_product_df.write \
        .parquet(f"/DataLake/silver/RobotDreamsAPI/Product-{last_partition_date}", mode='overwrite')
    logging.info(f"Success!!!")