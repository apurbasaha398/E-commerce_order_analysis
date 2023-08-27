# Order Analysis of E-commerce Data

### Software and tools requirements

1. Apache Hadoop
2. Apache Spark
3. Apache Kafka
4. MySql Server
5. Tableau Desktop

Please follow the instruction files in Docs folder to install the appropriate version of these softwares

### Steps to follow
1. Install all the softwares
2. Create a python environment (if not created yet)
   ``` 
   python -m venv /path/to/new/virtual/environment
   ```
3. Activate the environment
   ```
   source /path/to/new/virtual/environment/bin/activate
   ```
4. Make sure Hadoop components and Kafka is running in the backgroud using the following command.
   ```
   jps
   ```
5. If Hadoop components or Kafks is not running, start them using the following set of commands.
   ```
   start-dfs.sh
   start-yarn.sh
   sudo systemctl start zookeeper
   sudo systemctl start kafka
   ```
6. Create a Kafka topic named 'order-events' where the Kafka producers will send the order messages
   ```
   kafka-topics.sh --create --topic order-events --bootstrap-server localhost:9092
   ```
7. Make sure MySql server is also running in the background. If not, MySql wasn't installed properly.
   ```
   sudo systemctl status mysql
   ```
8. Clone this git repository to your local machine
   ```
   git clone <repository link>
   ```
9. Store all the data files inside the hdfs folder if not done before. Update the path of .csv files in data_ingestion.py and stream_data.py if data files are located in a different subfolder
   ```
   hdfs dfs -mkdir -p /path/to/files
   hdfs dfs -put /local-file-path /hdfs-file-path
   ```
10. Download the mysql connector from [this link](https://mvnrepository.com/artifact/mysql/mysql-connector-java). Also update the path of this jar file in data_ingestion.py. The path was used to initiate spark session. Without this file, spark won't be able to communicate with the mysql server.
    
11. Run the stream_data.py file to start streaming the orders
    ```
    python3 stream-data.py
    ```
12. Run the data_ingestion.py to load the streaming data to a mysql database
    ```
    python3 data_ingestion.py
    ```
13. Now open the .twb file inside Tableau Workbook folder to view the dashboard. Connect to the MySql Database using the credential you used for creating mysql server.
    As more streamed data gets uploaded to the mysql database, the views in the dashboard also changes dynamically.
