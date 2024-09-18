import mysql.connector
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging

class MySQLConnect:
    def __init__(self, config):
        self.config = config
        logging.info("MySQLConnect instance created with config: %s", self.config)

    def create_connect(self):
        """Tạo kết nối đến MySQL"""
        try:
            logging.info("Attempting to connect to MySQL database...")
            connection = mysql.connector.connect(
                host=self.config['host'],
                user=self.config['username'],
                password=self.config['password'],
                database=self.config['database'],
                port=int(self.config['port'])
            )
            logging.info("Successfully connected to MySQL database.")
            return connection
        except mysql.connector.Error as err:
            logging.error("Error: %s", err)
            raise

class CassandraConnect:
    def __init__(self, config):
        self.config = config
        logging.info("CassandraConnect instance created with config: %s", self.config)

    def create_connect(self):
        """Tạo kết nối đến Cassandra"""
        try:
            logging.info("Attempting to connect to Cassandra...")
            auth_provider = PlainTextAuthProvider(
                username=self.config['username'],
                password=self.config['password']
            )
            cluster = Cluster([self.config['host']], port=int(self.config['port']), auth_provider=auth_provider)
            session = cluster.connect(self.config['keyspace'])
            logging.info("Successfully connected to Cassandra keyspace '%s'.", self.config['keyspace'])
            return session
        except Exception as e:
            logging.error("Error: %s", e)
            raise
