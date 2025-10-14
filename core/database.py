"""
Database connection and query utilities for IRTS harvest system.
"""

from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import sqlite3
import structlog
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv('config.env')

logger = structlog.get_logger()


class DatabaseConnection:
    """Manages database connections and provides query utilities."""

    def __init__(self):
        self.db_path = os.getenv('DB_PATH', 'irts.sqlite')
        self._connection: Optional[sqlite3.Connection] = None
        self._initialized = False

    def connect(self) -> sqlite3.Connection:
        """Establish database connection."""
        if self._connection is None:
            self._connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            # Return rows as dictionaries
            self._connection.row_factory = sqlite3.Row
            # Enable foreign keys
            self._connection.execute("PRAGMA foreign_keys = ON")
            logger.info("database_connected", path=self.db_path)

            # Initialize schema on first connection
            if not self._initialized:
                self._initialize_schema()
                self._initialized = True

        return self._connection

    def _initialize_schema(self):
        """Initialize database schema from schema.sql if tables don't exist."""
        # Check if tables exist
        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'"
        )
        if cursor.fetchone() is None:
            logger.info("initializing_database_schema")
            schema_path = Path(__file__).parent.parent / 'schema.sql'
            if schema_path.exists():
                with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                self._connection.executescript(schema_sql)
                self._connection.commit()
                logger.info("database_schema_initialized")
            else:
                logger.warning("schema_file_not_found", path=str(schema_path))

    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
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
            cur.execute(query, params or ())
            return cur.rowcount

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
            # Convert sqlite3.Row objects to dictionaries
            return [dict(row) for row in cur.fetchall()]

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
        columns = ', '.join(f'"{col}"' for col in data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders})'

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
        set_clause = ', '.join(f'"{col}" = ?' for col in data.keys())
        where_clause = ' AND '.join(f'"{col}" = ?' for col in where.keys())
        query = f'UPDATE "{table}" SET {set_clause} WHERE {where_clause}'

        params = tuple(data.values()) + tuple(where.values())
        with self.cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount

    def escape_string(self, value: str) -> str:
        """
        Escape a string for SQL (for compatibility with MySQL code).

        Note: This method is provided for compatibility but should not be used.
        Always use parameterized queries instead.

        Args:
            value: String to escape

        Returns:
            Escaped string
        """
        # SQLite handles escaping through parameterized queries
        # This is just for compatibility with existing code
        return value.replace("'", "''")

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
