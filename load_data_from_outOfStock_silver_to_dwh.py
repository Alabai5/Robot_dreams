from datetime import date
import pyspark
import logging

from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def load_data_outOfStock_to_dwh():
    gp_url = BaseHook.get_connection('olap_greenplum_enterprise')
    gp_properties = {"user": "gpuser", "password": "secret"}

    logging.info(f"Create spark session")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared_folder/rotot_dreams/postgresql-42.2.23.jar') \
        .master('local') \
        .appName("FromDshopBuSilverToEnterpriseDWH") \
        .getOrCreate()
    outofstock_fs = spark.read.load(f"/DataLake/bronze/out_of_stock/2021-08-20/Product-2021-08-20.json"
                                    , header="true"
                                    , inferSchema="true"
                                    , format="json")
    outofstock_df = outofstock_fs.select(F.col('date').alias('dimdateid_df')
                                         , F.col('product_id').alias('dimproductid_df'))

    gp_factoutOfStock = spark.read.jdbc(gp_url
                                        , table='dwh.factoutofstock'
                                        , properties=gp_properties
                                        )
    outofstock_df = outofstock_df.dropDuplicates()

    load_factoutOfStock = outofstock_df.join(gp_factoutOfStock
                                             , (outofstock_df.dimdateid_df == gp_factoutOfStock.dimdateid) & (
                                                         outofstock_df.dimproductid_df == gp_factoutOfStock.dimproductid)
                                             , 'left') \
        .filter(gp_factoutOfStock.dimdateid.isNull()).select(F.col('dimdateid_df').alias('dimdateid')
                                                             , F.col('dimproductid_df').alias('dimproductid'))

    convert_load_factoutOfStock = load_factoutOfStock.withColumn("dimdateid",
                                                                 load_factoutOfStock["dimdateid"].cast(DateType())) \
        .withColumn("dimproductid", load_factoutOfStock["dimproductid"].cast(IntegerType()))

    convert_load_factoutOfStock.write.jdbc(gp_url
                                           , table='dwh.factoutofstock'
                                           , properties=gp_properties
                                           , mode='append')
    logging.info(f"Finish spark session")