import configparser
import logging
import os
from pyspark.sql import SparkSession

JDBC = os.path.join(os.path.dirname(__file__), "../jars/mysql-connector-j-9.0.0.jar")
SPARK_CASSANDRA_CONNECTOR = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"

def get_config(section):
    """Đọc cấu hình từ file /src/config/config.ini"""
    ini_file_path = os.path.join(os.path.dirname(__file__), '../config/config.ini')
    config = configparser.ConfigParser()
    config.read(ini_file_path)
    if section in config:
        return {key: value for key, value in config.items(section)}
    else:
        raise ValueError(f"No section: '{section}'")

def create_spark_session():
    """Tạo SparkSession với cấu hình Cassandra + MySQL JDBC"""
    try:
        cassandra_config = get_config('cassandra')
        logging.info("Creating SparkSession with Cassandra configuration...")
        spark = SparkSession.builder \
                            .appName("App") \
                            .config("spark.jars", JDBC) \
                            .config("spark.jars.packages", SPARK_CASSANDRA_CONNECTOR) \
                            .config("spark.cassandra.auth.username", cassandra_config['username']) \
                            .config("spark.cassandra.auth.password", cassandra_config['password']) \
                            .getOrCreate()
        logging.info("SparkSession successfully created.")
        return spark
    except Exception as e:
        logging.error("Error creating SparkSession: %s", e)
        raise



