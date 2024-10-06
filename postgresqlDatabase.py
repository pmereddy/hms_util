import psycopg2
from psycopg2 import sql
from psycopg2 import pool
import logging
import traceback
#from logging_setup import setup_logging

logger = logging.getLogger(__name__)

class PostgreSQLDatabase:
    def __init__(self):
        self.connection_pool = None
        self.logger = logger

    def connect(self, host, port, database, user, password):
        """Connect to the PostgreSQL database using a connection pool."""
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                dbname=database,
                user=user,
                password=password,
                host=host,
                port=port
            )
            if self.connection_pool:
                self.logger.info('Connected to PostgreSQL database at %s:%d', host, port)
            else:
                self.logger.error('Failed to create connection pool')
        except Exception as e:
            self.logger.error('Error connecting to PostgreSQL: %s', e)

    def disconnect(self):
        """Disconnect from the PostgreSQL database and close the connection pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.logger.info('Disconnected from PostgreSQL database and closed connection pool')

    def query(self, sql_query: str, params: tuple = ()):
        """Execute a query on the PostgreSQL database using a connection from the pool."""
        if not self.connection_pool:
            self.logger.error('Connection pool is not initialized')
            return None, None

        try:
            conn = self.connection_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                if cursor.description:
                    results = cursor.fetchall()
                    cols = [desc[0] for desc in cursor.description]
                    self.logger.debug('Query executed successfully: %s', sql_query)
                    return results, cols
                else:
                    conn.commit()
                    self.logger.info('Query executed successfully (no results): %s', sql_query)
                    return None, None
        except Exception as e:
            self.logger.error('Error executing query: %s', e)
            traceback.print_exc()
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                self.connection_pool.putconn(conn)
