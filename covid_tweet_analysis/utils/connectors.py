import socket
from pyspark.sql import DataFrame

class TCPConnector:

    def connect_to_socket(self, tcp_ip: str = "localhost", tcp_port: int = 9009):
        conn = None 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((tcp_ip, tcp_port))
        s.listen(1)
        print("Waiting for TCP connection...")
        conn, addr = s.accept()
        print("Connected... Starting getting tweets.")
        return conn

class CassandraConnector:

    def __init__(self):
        self.cassandra_package_name = "org.apache.spark.sql.cassandra"

    @staticmethod
    def write(data:DataFrame, table_name:str, keyspace_name:str, show=False):
        """Writes @data into the CassandraDB with said table and keyspace names"""
        data.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table=table_name, keyspace=keyspace_name)\
            .save()
        if (show is True):
            data.show()

