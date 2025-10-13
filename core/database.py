"""
Database connection and query utilities for IRTS harvest system.
"""

from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import pymysql
from pymysql.cursors import DictCursor
import structlog
import os
from dotenv import load_dotenv

load_dotenv('config.env')

logger = structlog.get_logger()


class DatabaseConnection:
    """Manages database connections and provides query utilities."""

    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = int(os.getenv('DB_PORT', '3306'))
        self.database = os.getenv('DB_NAME', 'irts')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self._connection: Optional[pymysql.Connection] = None

    def connect(self) -> pymysql.Connection:
        """Establish database connection."""
        if self._connection is None or not self._connection.open:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                cursorclass=DictCursor,
                autocommit=False
            )
            logger.info("database_connected", host=self.host, database=self.database)
        return self._connection

    def close(self):
        """Close database connection."""
        if self._connection and self._connection.open:
            self._connection.close()
            logger.info("database_closed")

    @contextmanager
    def cursor(self):
        """Context manager for database cursor."""
        conn = self.connect()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("database_error", error=str(e))
            raise
        finally:
            cur.close()

    def execute(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute a query and return affected rows.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Number of affected rows
        """
        with self.cursor() as cur:
            affected = cur.execute(query, params or ())
            return affected

    def query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return all results.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of result dictionaries
        """
        with self.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()

    def query_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Dict[str, Any]]:
        """
        Execute a SELECT query and return first result.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            First result dictionary or None
        """
        results = self.query(query, params)
        return results[0] if results else None

    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert a row into a table.

        Args:
            table: Table name
            data: Dictionary of column:value pairs

        Returns:
            Last insert ID
        """
        columns = ', '.join(f'`{col}`' for col in data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"

        with self.cursor() as cur:
            cur.execute(query, tuple(data.values()))
            return cur.lastrowid

    def update(self, table: str, data: Dict[str, Any], where: Dict[str, Any]) -> int:
        """
        Update rows in a table.

        Args:
            table: Table name
            data: Dictionary of column:value pairs to update
            where: Dictionary of column:value pairs for WHERE clause

        Returns:
            Number of affected rows
        """
        set_clause = ', '.join(f'`{col}` = %s' for col in data.keys())
        where_clause = ' AND '.join(f'`{col}` = %s' for col in where.keys())
        query = f"UPDATE `{table}` SET {set_clause} WHERE {where_clause}"

        params = tuple(data.values()) + tuple(where.values())
        with self.cursor() as cur:
            return cur.execute(query, params)

    def get_values(
        self,
        query: str,
        params: Optional[Tuple] = None,
        column: Optional[str] = None,
        single_value: bool = False
    ) -> Any:
        """
        Execute query and extract specific column values.

        Args:
            query: SQL query string
            params: Query parameters
            column: Column name to extract (if None, returns full rows)
            single_value: If True, return single value instead of list

        Returns:
            Single value, list of values, or list of dictionaries
        """
        results = self.query(query, params)

        if not results:
            return None if single_value else []

        if column:
            values = [row[column] for row in results if column in row]
            return values[0] if single_value and values else values

        return results[0] if single_value else results


# Global database instance
db = DatabaseConnection()
