import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
import logging
#from logging_setup import setup_logging

logger = logging.getLogger(__name__)
class MySQLDatabase:
    def __init__(self):
        self.connection_pool = None
        self.logger = logger

    def connect(self, host, port, database, user, password):
        """Initialize the connection pool for MySQL database."""
        try:
            self.connection_pool = pooling.MySQLConnectionPool(
                pool_name="mypool",
                pool_size=5,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            if self.connection_pool:
                self.logger.info(f"Connected to MySQL database at {host}:{port}")
            else:
                self.logger.error('Failed to create connection pool')
        except Error as e:
            self.logger.error(f"Error connecting to MySQL: {e}")

    def disconnect(self):
        if self.connection_pool:
            self.connection_pool.close()
            self.logger.info('Disconnected from MySQL database and closed connection pool')

    def query(self, sql_query: str, params: tuple = ()):
        """Execute a query on the MySQL database using a connection from the pool."""
        if not self.connection_pool:
            self.logger.error('Connection pool is not initialized')
            return None,None

        try:
            conn = self.connection_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql_query, params)
            if cursor.description:
                results = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                self.logger.debug("Query executed successfully: {sql_query}")
                return results, cols
            else:
                conn.commit()
                self.logger.info("Query executed successfully (no results): {sql_query}")
                return None, None
        except Exception as e:
            self.logger.error('Error executing query: %s', e)
            if conn:
                conn.rollback()
            return None, None
        finally:
            cursor.close()
            if conn:
                conn.close()
