import os
import logging
from queue import Queue
import threading
import vertica_python
from typing import Dict, Any, Optional, Set
from dataclasses import dataclass
from enum import Enum, auto

# Constants for environment variables
VERTICA_HOST = "VERTICA_HOST"
VERTICA_PORT = "VERTICA_PORT"
VERTICA_DATABASE = "VERTICA_DATABASE"
VERTICA_USER = "VERTICA_USER"
VERTICA_PASSWORD = "VERTICA_PASSWORD"
VERTICA_CONNECTION_LIMIT = "VERTICA_CONNECTION_LIMIT"
VERTICA_SSL = "VERTICA_SSL"
VERTICA_SSL_REJECT_UNAUTHORIZED = "VERTICA_SSL_REJECT_UNAUTHORIZED"

# Configure logging
logger = logging.getLogger("mcp-vertica")

class OperationType(Enum):
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    DDL = auto()

@dataclass
class SchemaPermissions:
    insert: bool = False
    update: bool = False
    delete: bool = False
    ddl: bool = False

@dataclass
class VerticaConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    connection_limit: int = 10
    ssl: bool = False
    ssl_reject_unauthorized: bool = True
    # Global operation permissions
    allow_insert: bool = False
    allow_update: bool = False
    allow_delete: bool = False
    allow_ddl: bool = False
    # Schema-specific permissions
    schema_permissions: Optional[Dict[str, SchemaPermissions]] = None

    def __post_init__(self):
        if self.schema_permissions is None:
            self.schema_permissions = {}

    @classmethod
    def from_env(cls) -> 'VerticaConfig':
        """Create config from environment variables."""
        # Parse schema permissions
        schema_permissions = {}
        for schema_perm in [
            ("SCHEMA_INSERT_PERMISSIONS", "insert"),
            ("SCHEMA_UPDATE_PERMISSIONS", "update"),
            ("SCHEMA_DELETE_PERMISSIONS", "delete"),
            ("SCHEMA_DDL_PERMISSIONS", "ddl")
        ]:
            env_var, perm_type = schema_perm
            if perm_str := os.getenv(env_var):
                for pair in perm_str.split(','):
                    schema, value = pair.split(':')
                    schema = schema.strip()
                    if schema not in schema_permissions:
                        schema_permissions[schema] = SchemaPermissions()
                    setattr(schema_permissions[schema], perm_type, value.strip().lower() == 'true')

        return cls(
            host=os.getenv("VERTICA_HOST", "localhost"),
            port=int(os.getenv("VERTICA_PORT", "5433")),
            database=os.getenv("VERTICA_DATABASE", "VMart"),
            user=os.getenv("VERTICA_USER", "newdbadmin"),
            password=os.getenv("VERTICA_PASSWORD", "vertica"),
            connection_limit=int(os.getenv("VERTICA_CONNECTION_LIMIT", "10")),
            ssl=os.getenv("VERTICA_SSL", "false").lower() == "true",
            ssl_reject_unauthorized=os.getenv("VERTICA_SSL_REJECT_UNAUTHORIZED", "true").lower() == "true",
            allow_insert=os.getenv("ALLOW_INSERT_OPERATION", "false").lower() == "true",
            allow_update=os.getenv("ALLOW_UPDATE_OPERATION", "false").lower() == "true",
            allow_delete=os.getenv("ALLOW_DELETE_OPERATION", "false").lower() == "true",
            allow_ddl=os.getenv("ALLOW_DDL_OPERATION", "false").lower() == "true",
            schema_permissions=schema_permissions
        )

class VerticaConnectionPool:
    def __init__(self, config: VerticaConfig):
        self.config = config
        self.pool: Queue = Queue(maxsize=config.connection_limit)
        self.active_connections = 0
        self.lock = threading.Lock()
        self._initialize_pool()

    def _get_connection_config(self) -> Dict[str, Any]:
        """Get connection configuration with SSL settings if enabled."""
        config = {
            "host": self.config.host,
            "port": self.config.port,
            "database": self.config.database,
            "user": self.config.user,
            "password": self.config.password,
        }

        if self.config.ssl:
            config["ssl"] = True
            config["ssl_reject_unauthorized"] = self.config.ssl_reject_unauthorized

        logger.debug("Connection config: %s", self._get_safe_config(config))
        return config

    def _get_safe_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a safe version of the config for logging by masking sensitive data."""
        safe_config = config.copy()
        if "password" in safe_config:
            safe_config["password"] = "********"
        return safe_config

    def _initialize_pool(self):
        """Initialize the connection pool with the specified number of connections."""
        logger.info(
            f"Initializing Vertica connection pool with {self.config.connection_limit} connections"
        )
        for _ in range(self.config.connection_limit):
            try:
                conn = vertica_python.connect(**self._get_connection_config())
                self.pool.put(conn)
            except Exception as e:
                logger.error(f"Failed to create connection: {str(e)}")
                raise

    def get_connection(self) -> vertica_python.Connection:
        """Get a connection from the pool."""
        with self.lock:
            if self.active_connections >= self.config.connection_limit:
                raise Exception("No available connections in the pool")

            try:
                conn = self.pool.get(timeout=5)  # 5 second timeout
                self.active_connections += 1
                return conn
            except Exception as e:
                logger.error(f"Failed to get connection from pool: {str(e)}")
                raise

    def release_connection(self, conn: vertica_python.Connection):
        """Release a connection back to the pool."""
        with self.lock:
            try:
                self.pool.put(conn)
                self.active_connections -= 1
            except Exception as e:
                logger.error(f"Failed to release connection to pool: {str(e)}")
                try:
                    conn.close()
                except:
                    pass

    def close_all(self):
        """Close all connections in the pool."""
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except:
                pass

class VerticaConnectionManager:
    def __init__(self):
        self.pool: Optional[VerticaConnectionPool] = None
        self.config: Optional[VerticaConfig] = None
        self.lock = threading.Lock()
        self.is_multi_db_mode: bool = False

    def initialize_default(self, config: VerticaConfig):
        """Initialize the connection pool."""
        self.config = config
        self.is_multi_db_mode = not config.database
        self.pool = VerticaConnectionPool(config)

    def get_connection(self) -> vertica_python.Connection:
        """Get a connection from the pool. Vertica does not support runtime database switching."""
        if not self.pool:
            raise Exception("Connection pool not initialized")
        conn = self.pool.get_connection()
        return conn

    def release_connection(self, conn: vertica_python.Connection):
        """Release a connection back to the pool."""
        if self.pool:
            self.pool.release_connection(conn)

    def is_operation_allowed(self, database: str, operation: OperationType) -> bool:
        """Check if an operation is allowed for a specific database."""
        if not self.config:
            return False

        # Get schema permissions
        schema_permissions = self.config.schema_permissions or {}
        schema_perms = schema_permissions.get(database)

        # Check schema-specific permissions first
        if schema_perms:
            if operation == OperationType.INSERT:
                return schema_perms.insert
            elif operation == OperationType.UPDATE:
                return schema_perms.update
            elif operation == OperationType.DELETE:
                return schema_perms.delete
            elif operation == OperationType.DDL:
                return schema_perms.ddl

        # Fall back to global permissions
        if operation == OperationType.INSERT:
            return self.config.allow_insert
        elif operation == OperationType.UPDATE:
            return self.config.allow_update
        elif operation == OperationType.DELETE:
            return self.config.allow_delete
        elif operation == OperationType.DDL:
            return self.config.allow_ddl

        return False

    def close_all(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.close_all()
