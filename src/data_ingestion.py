import os
import sys
from src.exception import CustomException
from src.logger import logging
from src.utils import get_kafka_config, save_to_mysql_table, create_mysql_database, create_mysql_table

import pandas as pd
import numpy as np
import time

SPARK_HOME = os.environ['SPARK_HOME']
import findspark
findspark.init(SPARK_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class DataIngestion:  
    def __init__(self):
        pass
    
    def initiate_data_injection(self):
        try:
            logging.info("Initiating data ingestion")
            scala_version = '2.12'
            spark_version = '3.3.1'
            # TODO: Ensure match above values match the correct versions
            packages = [
                f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
                'org.apache.kafka:kafka-clients:3.2.1'
            ]
            spark = SparkSession \
                    .builder \
                    .appName("Real-Time Data Processing with Kafka Source and Message Format as JSON") \
                    .config("spark.jars.packages", ','.join(packages)) \
                    .config('spark_jars', "file:////home/apurba/workarea/softwares/spark-3.3.1-bin-hadoop3/jars/mysql-connector-java-8.0.28.jar") \
                    .config("spark_excutor.extraClassPath", "file:////home/apurba/workarea/softwares/spark-3.3.1-bin-hadoop3/jars/mysql-connector-java-8.0.28.jar") \
                    .config("spark.driver.extraClassPath", "file:////home/apurba/workarea/softwares/spark-3.3.1-bin-hadoop3/jars/mysql-connector-java-8.0.28.jar") \
                    .config("spark.executor.extraLibrary", "file:////home/apurba/workarea/softwares/spark-3.3.1-bin-hadoop3/jars/mysql-connector-java-8.0.28.jar") \
                    .master("local[*]") \
                    .getOrCreate()

            spark.sparkContext.setLogLevel("ERROR")
            
            kafka_topic_name, kafka_bootstrap_server = get_kafka_config()
            orders_df = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
                .option("subscribe", kafka_topic_name) \
                .option("startingOffsets", "latest") \
                .load()
                
            logging.info("Creating mysql database")
            mysql_database_name = "olist"
            create_mysql_database(mysql_database_name)
                
            #print("Printing Schema of orders_df: ")
            #orders_df.printSchema()
            
            orders_df1 = orders_df.selectExpr("CAST(value AS STRING)")
            orders_schema = StructType() \
                .add("order_id", StringType()) \
                .add("customer_id", StringType()) \
                .add("order_status", StringType()) \
                .add("order_item_id", StringType()) \
                .add("product_id", StringType()) \
                .add("seller_id", StringType()) \
                .add("price", FloatType()) \
                .add("freight_value", FloatType()) \
                .add("order_purchase_timestamp", TimestampType()) \
                .add("order_delivered_customer_date", TimestampType()) \
                .add("order_estimated_delivery_date", TimestampType())
                
            orders_df2 = orders_df1\
                .select(from_json(col("value"), orders_schema)\
                .alias("orders"))
            #orders_df2.printSchema()
            
            orders_df3 = orders_df2.select("orders.*")
            
            olist_customer_data_path = os.path.join("hdfs://localhost:9000", "data", "olist", "olist_customers_dataset.csv")
            olist_customer_df= spark.read.csv(olist_customer_data_path, header=True, inferSchema=True)
            olist_customer_df = olist_customer_df.select("customer_id", "customer_city", "customer_state")
            
            olist_product_data_path = os.path.join("hdfs://localhost:9000", "data", "olist", "olist_products_dataset.csv")
            olist_product_df = spark.read.csv(olist_product_data_path, header=True, inferSchema=True)
            olist_product_df= olist_product_df.select("product_id", "product_category_name")
            
            olist_translate_data_path = os.path.join("hdfs://localhost:9000", "data", "olist", "product_category_name_translation.csv")
            olist_translate_df = spark.read.csv(olist_translate_data_path, header=True, inferSchema=True)
            translation_map = olist_translate_df.rdd.collectAsMap()

            # Define and register the UDF
            translate_udf = udf(lambda x: translation_map.get(x), StringType())
            spark.udf.register("translate_udf", translate_udf)

            # Use the UDF to translate the original_text column
            olist_product_df = olist_product_df.withColumn("product_category_name", translate_udf(col("product_category_name")))
            #olist_product_df.show(5, truncate=False)
            
            olist_seller_data_path = os.path.join("hdfs://localhost:9000", "data", "olist", "olist_sellers_dataset.csv")
            olist_seller_df = spark.read.csv(olist_seller_data_path, header=True, inferSchema=True)
            olist_seller_df = olist_seller_df.select("seller_id", "seller_city", "seller_state")
            
            olist_review_data_path = os.path.join("hdfs://localhost:9000", "data", "olist", "olist_order_reviews_dataset.csv")
            olist_review_df = spark.read.csv(olist_review_data_path, header=True, inferSchema=True)
            olist_review_df = olist_review_df.select("order_id", "review_score")
            
            final_df = orders_df3.join(olist_customer_df, "customer_id", "left")\
                .join(olist_product_df, "product_id", "left")\
                .join(olist_seller_df, "seller_id", "left")\
                .join(olist_review_df, "order_id", "left")
                
            # Extract order hour
            final_df = final_df.withColumn("order_hour", hour(col("order_purchase_timestamp")))
            final_df = final_df.withColumn("order_date", to_date(col("order_purchase_timestamp")))
            final_df = final_df.withColumn("total_price", col("price") + col("freight_value"))
            
            # Remove the columns which are not required
            final_df = final_df.drop("price", "freight_value", "order_purchase_timestamp")
            
            # Change the data type of 'review_score' column to Integer
            final_df = final_df.withColumn("review_score", col("review_score").cast(IntegerType()))
            final_df.printSchema()
            
            logging.info("Creating mysql table")
            mysql_table_name = "order_data"
            create_mysql_table(final_df, mysql_database_name, mysql_table_name)
            
            order_detail_write_stream = final_df \
                .writeStream \
                .outputMode("update") \
                .foreachBatch(lambda current_df, epoc_id: save_to_mysql_table(
                                                                    current_df, 
                                                                    epoc_id, 
                                                                    mysql_table_name, 
                                                                    mysql_database_name)) \
                .trigger(processingTime='2 seconds') \
                .start()
                
            print(order_detail_write_stream.status)
            time.sleep(3600)
            print(order_detail_write_stream.status)
            order_detail_write_stream.stop()              
            logging.info("Data ingestion completed")
            
        except Exception as e:
            logging.error("Error while ingesting data")
            raise CustomException(e, sys)
        
if __name__ == "__main__":
    data_ingestion = DataIngestion()
    data_ingestion.initiate_data_injection()