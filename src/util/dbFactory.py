from util.dbConnect import MySQLConnect, CassandraConnect
from util.util import get_config

class DatabaseFactory:
    def __init__(self):
        # Định nghĩa các loại kết nối sẵn có
        self.connections = {
            'mysql': MySQLConnect,
            'cassandra': CassandraConnect
        }

    def create_connect(self, db_type):
        """Tạo kết nối dựa trên loại cơ sở dữ liệu"""
        db_type = db_type.lower()
        
        # Lấy lớp kết nối từ dictionary
        connection_class = self.connections.get(db_type)
        
        if not connection_class:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        # Đọc cấu hình và tạo kết nối
        config = get_config(db_type)
        return connection_class(config).create_connect()
