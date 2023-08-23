import os
import sys
import pandas as pd
from src.exception import CustomException
from src.logger import logging

from kafka import KafkaProducer
from json import dumps
import time
from configparser import ConfigParser
           
logging.info("Loading Kafka Cluster/Server details from configuration file(kafka.conf)")

conf_file_path = os.path.join("config", "kafka.conf")
config_obj = ConfigParser()
config_obj.read(conf_file_path)

# Kafka Cluster/Server Details
kafka_host_name = config_obj.get('kafka', 'host')
kafka_port_no = config_obj.get('kafka', 'port_no')
kafka_topic_name = config_obj.get('kafka', 'input_topic_name')

KAFKA_TOPIC_NAME_CONS = kafka_topic_name
KAFKA_BOOTSTRAP_SERVERS_CONS = kafka_host_name + ':' + kafka_port_no

order_data_path = os.path.join("notebook", "Data", "olist_orders_dataset.csv")
order_items_path = os.path.join("notebook", "Data", "olist_order_items_dataset.csv")

# Read the CSV file and select specific columns
order_df = pd.read_csv(order_data_path, \
            usecols=["order_id", "customer_id", "order_status", "order_purchase_timestamp", 
                     "order_delivered_customer_date", "order_estimated_delivery_date"])
order_items_df = pd.read_csv(order_items_path, \
                            usecols=["order_id", "product_id", "order_item_id", "seller_id", "price", "freight_value"])

order_df["order_purchase_timestamp"] = pd.to_datetime(order_df["order_purchase_timestamp"])

# Sort the DataFrame based on the order_purchase_timestamp
order_df.sort_values(by="order_purchase_timestamp", inplace=True)

# Convert the order_purchase_timestamp back to string format
order_df["order_purchase_timestamp"] = order_df["order_purchase_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

merged_df = order_df.merge(order_items_df, how='left', on="order_id")

orders_list = merged_df.to_dict('records')

logging.info("Starting streaming the data")
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
for order in orders_list:
    print("Message to be sent: ", order)
    kafka_producer.send(KAFKA_TOPIC_NAME_CONS, value=order)
    time.sleep(1)