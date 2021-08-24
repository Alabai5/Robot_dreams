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


def load_tables_dshop_from_silver_to_dwh(values):
    gp_url = BaseHook.get_connection('olap_greenplum_enterprise')
    gp_properties = {"user": "gpuser", "password": "secret"}

    logging.info(f"Create spark session")

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared_folder/rotot_dreams/postgresql-42.2.23.jar') \
        .master('local') \
        .appName("FromDshopBuSilverToEnterpriseDWH") \
        .getOrCreate()
    if values == 'location_areas':
        location_areas_fs = spark.read.parquet(f"/DataLake/silver/dshop_bu/location_areas")
        location_areas_df = location_areas_fs.select(F.col('area_id').alias('id_df')
                                                     , F.col('area').alias('areaname_df'))
        gp_locationarea = spark.read.jdbc(gp_url
                                          , table='dwh.dimlocationarea'
                                          , properties=gp_properties
                                          )
        load_locationarea = location_areas_df.join(gp_locationarea, location_areas_df.id_df == gp_locationarea.id,
                                                   'left') \
            .filter(gp_locationarea.id.isNull()).select(F.col('id_df').alias('id'),
                                                        F.col('areaname_df').alias('areaname'))
        load_locationarea.write.jdbc(gp_url
                                     , table='dwh.dimlocationarea'
                                     , properties=gp_properties
                                     , mode='append')
    elif values == 'aisles':
        aisles_fs = spark.read.parquet(f"/DataLake/silver/dshop_bu/aisles")
        aisles_df = aisles_fs.select(F.col('aisle_id').alias('id_df'), F.col('aisle').alias('aislesname_df'))
        gp_aisles = spark.read.jdbc(gp_url
                                    , table='dwh.dimaisles'
                                    , properties=gp_properties
                                    )
        load_aisles = aisles_df.join(gp_aisles, aisles_df.id_df == gp_aisles.id, 'left') \
            .filter(gp_aisles.id.isNull()).select(F.col('id_df').alias('id'),
                                                  F.col('aislesname_df').alias('aislesname'))

        load_aisles.write.jdbc(gp_url
                               , table='dwh.dimaisles'
                               , properties=gp_properties
                               , mode='append')
    elif values == 'clients':
        clients_fs = spark.read.parquet(f"/DataLake/silver/dshop_bu/clients")
        clients_df = clients_fs.select(F.col('id').alias('id_df')
                                       , F.col('fullname').alias('clientname_df')
                                       , F.col('location_area_id').alias('dimlocationareaid_df'))

        gp_clients = spark.read.jdbc(gp_url
                                     , table='dwh.dimclient'
                                     , properties=gp_properties
                                     )
        load_clients = clients_df.join(gp_clients 
                                       , clients_df.id_df == gp_clients.id
                                       , 'left') \
            .filter(gp_clients.id.isNull()).select(F.col('id_df').alias('id'),
                                                   F.col('clientname_df').alias('clientname')
                                                   , F.col('dimlocationareaid_df').alias('dimlocationareaid'))

        load_clients.write.jdbc(gp_url
                                , table='dwh.dimclient'
                                , properties=gp_properties
                                , mode='append')
    elif values == 'store_types':
        store_types_fs = spark.read.parquet(f"/DataLake/silver/dshop_bu/store_types")
        store_types_df = store_types_fs.select(F.col('store_type_id').alias('id_df')
                                               , F.col('type').alias('storetype_df'))

        gp_store_types = spark.read.jdbc(gp_url
                                         , table='dwh.dimstoretype'
                                         , properties=gp_properties
                                         )

        load_store_types = store_types_df.join(gp_store_types
                                               , store_types_df.id_df == gp_store_types.id
                                               , 'left') \
            .filter(gp_store_types.id.isNull()).select(F.col('id_df').alias('id'),
                                                       F.col('storetype_df').alias('storetype'))

        load_store_types.write.jdbc(gp_url
                                    , table='dwh.dimstoretype'
                                    , properties=gp_properties
                                    , mode='append')
    elif values == 'stores':
        stores_fs = spark.read.parquet(f"/DataLake/silver/dshop_bu/stores")
        stores_df = stores_fs.select(F.col('store_id').alias('id_df')
                                     , F.col('location_area_id').alias('dimlocationareaid_df')
                                     , F.col('store_type_id').alias('dimstoretypeid_df'))

        gp_stores = spark.read.jdbc(gp_url
                                    , table='dwh.dimstore'
                                    , properties=gp_properties
                                    )

        load_stores = stores_df.join(gp_stores
                                     , stores_df.id_df == gp_stores.id
                                     , 'left') \
            .filter(gp_stores.id.isNull()).select(F.col('id_df').alias('id'),
                                                  F.col('dimlocationareaid_df').alias('dimlocationareaid')
                                                  , F.col('dimstoretypeid_df').alias('dimstoretypeid'))

        load_stores.write.jdbc(gp_url
                               , table='dwh.dimstore'
                               , properties=gp_properties
                               , mode='append')
    elif values == 'departments':
        departments_fs = spark.read.parquet(f"/DataLake/silver/dshop_bu/departments")
        departments_df = departments_fs.select(F.col('department_id').alias('id_df')
                                               , F.col('department').alias('departmentname_df'))

        gp_departments = spark.read.jdbc(gp_url
                                         , table='dwh.dimdepartment'
                                         , properties=gp_properties
                                         )
        load_departments = departments_df.join(gp_departments
                                               , departments_df.id_df == gp_departments.id
                                               , 'left') \
            .filter(gp_departments.id.isNull()).select(F.col('id_df').alias('id'),
                                                       F.col('departmentname_df').alias('departmentname'))

        load_departments.write.jdbc(gp_url
                                    , table='dwh.dimdepartment'
                                    , properties=gp_properties
                                    , mode='append')
    elif values == 'products':
        products_fs = spark.read.parquet(f"/DataLake/silver/dshop_bu/products")
        products_df = products_fs.select(F.col('product_id').alias('id_df')
                                         , F.col('product_name').alias('productname_df')
                                         , F.col('aisle_id').alias('dimaisleid_df')
                                         , F.col('department_id').alias('dimdepartmentid_df'))

        gp_products = spark.read.jdbc(gp_url
                                      , table='dwh.dimproduct'
                                      , properties=gp_properties
                                      )
        # schema = [i for i in gp_products.schema]

        # load_products =StructType(schema)

        load_products = products_df.join(gp_products
                                         , products_df.id_df == gp_products.id
                                         , 'left') \
            .filter(gp_products.id.isNull()).select(F.col('id_df').alias('id'),
                                                    F.col('productname_df').alias('productname')
                                                    , F.col('dimaisleid_df').alias('dimaisleid')
                                                    , F.col('dimdepartmentid_df').alias('dimdepartmentid'))

        convert_load_products = load_products.withColumn("dimaisleid", load_products["dimaisleid"].cast(IntegerType())) \
            .withColumn("dimdepartmentid", load_products["dimdepartmentid"].cast(IntegerType()))

        convert_load_products.printSchema()
        convert_load_products.write.jdbc(gp_url
                                         , table='dwh.dimproduct'
                                         , properties=gp_properties
                                         , mode='append')
    elif values == 'orders':
        orders_fs = spark.read.parquet(f"/DataLake/silver/dshop_bu/orders")
        orders_df = orders_fs.select(F.col('order_id').alias('id_df'))

        gp_orders = spark.read.jdbc(gp_url
                                    , table='dwh.dimorder'
                                    , properties=gp_properties
                                    )

        orders_df1 = orders_df.dropDuplicates()

        load_orders = orders_df1.join(gp_orders
                                      , orders_df1.id_df == gp_orders.id
                                      , 'left') \
            .filter(gp_orders.id.isNull()).select(F.col('id_df').alias('id'))

        load_orders.write.jdbc(gp_url
                               , table='dwh.dimorder'
                               , properties=gp_properties
                               , mode='append')
        factorders_df = orders_fs.select(F.col('order_id').alias('dimorderid_df')
                                         , F.col('product_id').alias('dimproductid_df')
                                         , F.col('client_id').alias('dimclientid_df')
                                         , F.col('store_id').alias('dimstoreid_df')
                                         , F.col('quantity').alias('quantity_df')
                                         , F.col('order_date').alias('dimdateid_df'))

        gp_factorders = spark.read.jdbc(gp_url
                                        , table='dwh.factorder'
                                        , properties=gp_properties
                                        )

        load_factorders = factorders_df.join(gp_factorders
                                             , (factorders_df.dimorderid_df == gp_factorders.dimorderid) & (
                                                         factorders_df.dimdateid_df == gp_factorders.dimdateid)
                                             & (factorders_df.dimclientid_df == gp_factorders.dimclientid) & (
                                                         factorders_df.dimproductid_df == gp_factorders.dimproductid)
                                             & (factorders_df.dimstoreid_df == gp_factorders.dimstoreid)
                                             , 'left') \
            .filter(gp_factorders.dimorderid.isNull()).select(F.col('dimorderid_df').alias('dimorderid')
                                                              , F.col('dimproductid_df').alias('dimproductid')
                                                              , F.col('dimclientid_df').alias('dimclientid')
                                                              , F.col('dimstoreid_df').alias('dimstoreid')
                                                              , F.col('dimdateid_df').alias('dimdateid')
                                                              , F.col('quantity_df').alias('quantity'))
        convert_load_factorders = load_factorders.withColumn("dimdateid", load_factorders["dimdateid"].cast(DateType()))
        convert_load_factorders.printSchema()
        convert_load_factorders.write.jdbc(gp_url
                                           , table='dwh.factorder'
                                           , properties=gp_properties
                                           , mode='append')
    else:
        logging.info(f"table does not exist")
