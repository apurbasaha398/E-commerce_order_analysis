from json import dumps
import os
from configparser import ConfigParser
import mysql.connector

SPARK_HOME = os.environ['SPARK_HOME']
import findspark
findspark.init(SPARK_HOME)
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_kafka_config():
    conf_file_path = os.path.join("config", "kafka.conf")
    config_obj = ConfigParser()
    config_obj.read(conf_file_path)

    # Kafka Cluster/Server Details
    kafka_host_name = config_obj.get('kafka', 'host')
    kafka_port_no = config_obj.get('kafka', 'port_no')
    kafka_topic_name = config_obj.get('kafka', 'input_topic_name')
    kafka_bootstrap_server = kafka_host_name + ':' + kafka_port_no
    return kafka_topic_name, kafka_bootstrap_server

def get_sql_config():
    conf_file_path = os.path.join("config", "mysql.conf")
    config_obj = ConfigParser()
    config_obj.read(conf_file_path)

    # MySQL Database Server Details
    mysql_host_name = config_obj.get('mysql', 'host')
    mysql_port_no = config_obj.get('mysql', 'port_no')
    mysql_user_name = config_obj.get('mysql', 'username')
    mysql_password = config_obj.get('mysql', 'password')
    mysql_driver = config_obj.get('mysql', 'driver')
    
    return mysql_host_name, mysql_port_no, mysql_user_name, mysql_password, mysql_driver

def create_mysql_database(mysql_database_name):
    mysql_host_name, mysql_port_no, mysql_user_name, mysql_password, mysql_driver = get_sql_config()
    
    db_params = {
        "host": mysql_host_name,
        "user": mysql_user_name,
        "password": mysql_password
    }

    # Connect to MySQL
    connection = mysql.connector.connect(**db_params)
    cursor = connection.cursor()

    # Create a new database
    database_name = mysql_database_name
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")

    # Close the MySQL connection
    cursor.close()
    connection.close()
    
def create_mysql_table(current_df, mysql_database_name, mysql_table_name):
    mysql_host_name, mysql_port_no, mysql_user_name, mysql_password, mysql_driver = get_sql_config()
    # Get DataFrame schema
    schema = current_df.schema
    #print(schema)
  
    db_params = {
        "host": mysql_host_name,
        "user": mysql_user_name,
        "password": mysql_password,
        "database": mysql_database_name
    }

    # Connect to MySQL
    connection = mysql.connector.connect(**db_params)
    cursor = connection.cursor()
 
    # Create a new table
    table_name = mysql_table_name
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
      
    # Generate the CREATE TABLE query based on DataFrame schema
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("

    for col in schema:
        col_name = col.name
        #print(col_name)
        col_type = None
        
        # Map Spark data types to MySQL data types
        if col.dataType.simpleString() == "string":
            col_type = "VARCHAR(255)"
        elif col.dataType.simpleString() == "float":
            col_type = "FLOAT"
        elif col.dataType.simpleString() == "int":
            col_type = "INT"
        elif col.dataType.simpleString() == "timestamp":
            col_type = "TIMESTAMP"
        elif col.dataType.simpleString() == "date":
            col_type = "DATE"
        elif col.dataType.simpleString() == "boolean":
            col_type = "BOOLEAN"
            
        #print(col.dataType.simpleString())
        if col_type:
            create_table_query += f"{col_name} {col_type}, "

    create_table_query = create_table_query.rstrip(", ") + ")"
    #print(create_table_query)
    cursor.execute(create_table_query)
    connection.commit()

    # Close the MySQL connection
    cursor.close()
    connection.close()

def save_to_mysql_table(current_df, epoc_id, mysql_table_name, mysql_database_name):
    print("Inside save_to_mysql_table function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing mysql_table_name: " + mysql_table_name)
    
    mysql_host_name, mysql_port_no, mysql_user_name, mysql_password, mysql_driver = get_sql_config()
    
    connection_properties = {
        "user": mysql_user_name,
        "password": mysql_password,
        "driver": mysql_driver  # MySQL JDBC driver class
    }

    mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + str(mysql_port_no) + "/" + mysql_database_name
    
    # change all the null values to None
    current_df = current_df.replace(float('nan'), None)

    #Save the dataframe to the table.
    current_df.write.jdbc(url = mysql_jdbc_url,
                  table = mysql_table_name,
                  mode = 'append',
                  properties = connection_properties)

    print("Exit out of save_to_mysql_table function")