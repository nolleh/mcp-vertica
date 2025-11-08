from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from mcp.server.fastmcp import FastMCP, Context
from typing import Any, List
import logging
import re
import os
from .__about__ import __version__
from .connection import VerticaConnectionManager, VerticaConfig, OperationType, VERTICA_HOST, VERTICA_PORT, VERTICA_DATABASE, VERTICA_USER, VERTICA_PASSWORD, VERTICA_CONNECTION_LIMIT, VERTICA_SSL, VERTICA_SSL_REJECT_UNAUTHORIZED
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import uvicorn
import csv
import io

# Configure logging
logger = logging.getLogger("mcp-vertica")

# Log version at module load time
logger.info(f"Loading mcp_vertica version: {__version__}")


class ConfigMiddleware(BaseHTTPMiddleware):
    """Middleware to parse Smithery config from URL parameters and set environment variables."""

    # Mapping from Smithery config keys to environment variables
    CONFIG_MAPPING = {
        "host": VERTICA_HOST,
        "dbPort": VERTICA_PORT,
        "database": VERTICA_DATABASE,
        "user": VERTICA_USER,
        "password": VERTICA_PASSWORD,
        "connectionLimit": VERTICA_CONNECTION_LIMIT,
        "ssl": VERTICA_SSL,
        "sslRejectUnauthorized": VERTICA_SSL_REJECT_UNAUTHORIZED,
    }

    async def dispatch(self, request: Request, call_next):
        """Parse URL parameters and set environment variables before processing request."""
        # Get query parameters
        params = dict(request.query_params)

        # Map config parameters to environment variables
        for config_key, env_var in self.CONFIG_MAPPING.items():
            if config_key in params:
                value = params[config_key]
                # Convert boolean strings
                if isinstance(value, str):
                    if value.lower() in ("true", "false"):
                        value = value.lower()
                os.environ[env_var] = str(value)
                logger.debug(f"Set {env_var}={value} from URL parameter {config_key}")

        response = await call_next(request)
        return response


async def get_or_create_manager(ctx: Context) -> VerticaConnectionManager | None:
    """Get connection manager from context or create it lazily.

    Args:
        ctx: FastMCP context

    Returns:
        VerticaConnectionManager instance or None if creation fails
    """
    manager = ctx.request_context.lifespan_context.get("vertica_manager")
    if not manager:
        try:
            manager = VerticaConnectionManager()
            config = VerticaConfig.from_env()
            manager.initialize_default(config)
            await ctx.info("Connection manager initialized from request config")
        except Exception as e:
            await ctx.error(f"Failed to initialize database connection: {str(e)}")
            return None
    return manager


def extract_operation_type(query: str) -> OperationType | None:
    """Extract the operation type from a SQL query."""
    query = query.strip().upper()

    if query.startswith("INSERT"):
        return OperationType.INSERT
    elif query.startswith("UPDATE"):
        return OperationType.UPDATE
    elif query.startswith("DELETE"):
        return OperationType.DELETE
    elif any(query.startswith(op) for op in ["CREATE", "ALTER", "DROP", "TRUNCATE"]):
        return OperationType.DDL
    return None


def extract_schema_from_query(query: str) -> str | None:
    """Extract schema name from a SQL query."""
    # database.table 또는 schema.table 패턴에서 schema 추출
    match = re.search(r"([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+", query)
    if match:
        return match.group(1)
    return None


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    """Server lifespan context manager that handles initialization and cleanup.

    Args:
        server: FastMCP server instance

    Yields:
        Dictionary containing the Vertica connection manager (may be None if env vars not set)
    """
    manager = None
    try:
        # Try to initialize connection manager from environment variables
        # This works for stdio clients (env vars or CLI args provided)
        # For Smithery (http), env vars are set via ConfigMiddleware on each request
        try:
            manager = VerticaConnectionManager()
            config = VerticaConfig.from_env()
            manager.initialize_default(config)
            logger.info("Vertica connection manager initialized successfully at startup")
        except Exception as e:
            # Not an error - config might come later via URL parameters (Smithery)
            logger.info(f"Connection manager not initialized at startup (will be lazy-loaded if needed): {str(e)}")

        yield {"vertica_manager": manager}
    finally:
        # Cleanup resources
        if manager:
            try:
                manager.close_all()
                logger.info("Vertica connection manager closed")
            except Exception as e:
                logger.error(f"Error during cleanup: {str(e)}")


# Create FastMCP instance with SSE support
logger.info(f"Creating FastMCP instance with version: {__version__}")
mcp = FastMCP(
    "Vertica Service",
    dependencies=["vertica-python", "pydantic", "starlette", "uvicorn"],
    lifespan=server_lifespan,
)
# Manually set the version on the underlying MCP server
# FastMCP doesn't pass version to the Server, so we set it directly
mcp._mcp_server.version = __version__
logger.info(f"FastMCP instance created with version: {mcp._mcp_server.version}")


async def run_sse(port: int = 8000) -> None:
    """Run the MCP server with SSE transport.

    Args:
        port: Port to listen on for SSE transport
    """
    starlette_app = Starlette(routes=[Mount("/", app=mcp.sse_app())])
    config = uvicorn.Config(starlette_app, host="0.0.0.0", port=port)  # noqa: S104
    app = uvicorn.Server(config)
    await app.serve()


def run_http(port: int = 8000) -> None:
    """Run the MCP server with streamable HTTP transport.

    Args:
        port: Port to listen on for HTTP transport (default: 8000)
             In Smithery deployment, PORT env var will override this
    """
    logger.info("Vertica MCP Server starting in HTTP mode...")

    # Setup Starlette app with CORS for cross-origin requests
    app = mcp.streamable_http_app()

    # Add config middleware to parse Smithery URL parameters
    # This must be added before CORS to ensure env vars are set early
    app.add_middleware(ConfigMiddleware)

    # IMPORTANT: add CORS middleware for browser based clients
    # Note: allow_credentials=False to work with allow_origins=["*"]
    # This is required for Smithery scanner to work properly
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,  # Changed from True to work with wildcard origins
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["mcp-session-id", "mcp-protocol-version"],
        max_age=86400,
    )

    logger.info(f"Listening on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="debug")


@mcp.tool()
async def execute_query(ctx: Context, query: str) -> str:
    """Execute a SQL query and return the results.

    Args:
        ctx: FastMCP context for progress reporting and logging
        query: SQL query to execute
        database: Optional database name to execute the query against

    Returns:
        Query results as a string
    """
    await ctx.info(f"Executing query: {query}")

    # Get or create connection manager
    manager = await get_or_create_manager(ctx)
    if not manager:
        return "Error: Failed to initialize database connection. Check configuration."

    # Extract schema from query if not provided
    schema = extract_schema_from_query(query)
    # Check operation permissions
    operation = extract_operation_type(query)
    if operation and not manager.is_operation_allowed(schema or "default", operation):
        error_msg = f"Operation {operation.name} not allowed for schema {schema}"
        await ctx.error(error_msg)
        return error_msg

    conn = None
    cursor = None
    try:
        conn = manager.get_connection()  # Always use default DB connection
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        await ctx.info(f"Query executed successfully, returned {len(results)} rows")
        return str(results)
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}"
        await ctx.error(error_msg)
        return error_msg
    finally:
        if cursor:
            cursor.close()
        if conn:
            manager.release_connection(conn)


@mcp.tool()
async def stream_query(
    ctx: Context, query: str, batch_size: int = 1000
) -> str:
    """Execute a SQL query and return the results in batches as a single string.

    Args:
        ctx: FastMCP context for progress reporting and logging
        query: SQL query to execute
        batch_size: Number of rows to fetch at once

    Returns:
        Query results as a concatenated string
    """
    await ctx.info(f"Executing query with batching: {query}")

    # Get or create connection manager
    manager = await get_or_create_manager(ctx)
    if not manager:
        return "Error: Failed to initialize database connection. Check configuration."

    # Extract schema from query if not provided
    schema = extract_schema_from_query(query)
    # Check operation permissions
    operation = extract_operation_type(query)
    if operation and not manager.is_operation_allowed(schema or "default", operation):
        error_msg = f"Operation {operation.name} not allowed for schema {schema}"
        await ctx.error(error_msg)
        return error_msg

    conn = None
    cursor = None
    try:
        conn = manager.get_connection()  # Always use default DB connection
        cursor = conn.cursor()
        cursor.execute(query)

        all_results = []
        total_rows = 0
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            total_rows += len(batch)
            await ctx.debug(f"Fetched {total_rows} rows")
            all_results.extend(batch)

        await ctx.info(f"Query completed, total rows: {total_rows}")
        return str(all_results)
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}"
        await ctx.error(error_msg)
        return error_msg
    finally:
        if cursor:
            cursor.close()
        if conn:
            manager.release_connection(conn)


@mcp.tool()
async def copy_data(
    ctx: Context, schema: str, table: str, data: List[List[Any]],
) -> str:
    """Copy data into a Vertica table using COPY command.

    Args:
        ctx: FastMCP context for progress reporting and logging
        schema: vertica schema to execute the copy against
        table: Target table name
        data: List of rows to insert

    Returns:
        Status message indicating success or failure
    """
    await ctx.info(f"Copying {len(data)} rows to table: {table}")

    # Get or create connection manager
    manager = await get_or_create_manager(ctx)
    if not manager:
        return "Error: Failed to initialize database connection. Check configuration."

    # Check operation permissions
    if not manager.is_operation_allowed(schema, OperationType.INSERT):
        error_msg = f"INSERT operation not allowed for database {schema}"
        await ctx.error(error_msg)
        return error_msg

    conn = None
    cursor = None
    try:
        conn = manager.get_connection()
        cursor = conn.cursor()

        # Convert data to CSV string
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(data)
        output.seek(0)

        # Create COPY command
        copy_query = f"""COPY {table} FROM STDIN DELIMITER ',' ENCLOSED BY '\"'"""
        cursor.copy(copy_query, output.getvalue())
        conn.commit()

        success_msg = f"Successfully copied {len(data)} rows to {table}"
        await ctx.info(success_msg)
        return success_msg
    except Exception as e:
        error_msg = f"Error copying data: {str(e)}"
        await ctx.error(error_msg)
        return error_msg
    finally:
        if cursor:
            cursor.close()
        if conn:
            manager.release_connection(conn)


@mcp.tool()
async def get_table_structure(
    ctx: Context,
    table_name: str,
    schema: str = "public"
) -> str:
    """Get the structure of a table including columns, data types, and constraints.

    Args:
        ctx: FastMCP context for progress reporting and logging
        table_name: Name of the table to inspect
        schema: Schema name (default: public)

    Returns:
        Table structure information as a string
    """
    await ctx.info(f"Getting structure for table: {schema}.{table_name}")

    # Get or create connection manager
    manager = await get_or_create_manager(ctx)
    if not manager:
        return "Error: Failed to initialize database connection. Check configuration."

    query = """
    SELECT
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable,
        column_default
    FROM v_catalog.columns
    WHERE table_schema = %s
    AND table_name = %s
    ORDER BY ordinal_position;
    """

    conn = None
    cursor = None
    try:
        conn = manager.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (schema, table_name))
        columns = cursor.fetchall()

        if not columns:
            return f"No table found: {schema}.{table_name}"

        # Get constraints
        cursor.execute("""
            SELECT
                constraint_name,
                constraint_type,
                column_name
            FROM v_catalog.constraint_columns
            WHERE table_schema = %s
            AND table_name = %s;
        """, (schema, table_name))
        constraints = cursor.fetchall()

        # Format the output
        result = f"Table Structure for {schema}.{table_name}:\n\n"
        result += "Columns:\n"
        for col in columns:
            result += f"- {col[0]}: {col[1]}"
            if col[2]:  # character_maximum_length
                result += f"({col[2]})"
            elif col[3]:  # numeric_precision
                result += f"({col[3]},{col[4]})"
            result += f" {'NULL' if col[5] == 'YES' else 'NOT NULL'}"
            if col[6]:  # column_default
                result += f" DEFAULT {col[6]}"
            result += "\n"

        if constraints:
            result += "\nConstraints:\n"
            for const in constraints:
                result += f"- {const[0]} ({const[1]}): {const[2]}\n"

        return result

    except Exception as e:
        error_msg = f"Error getting table structure: {str(e)}"
        await ctx.error(error_msg)
        return error_msg
    finally:
        if cursor:
            cursor.close()
        if conn:
            manager.release_connection(conn)


@mcp.tool()
async def list_indexes(
    ctx: Context,
    table_name: str,
    schema: str = "public"
) -> str:
    """List all indexes for a specific table.

    Args:
        ctx: FastMCP context for progress reporting and logging
        table_name: Name of the table to inspect
        schema: Schema name (default: public)

    Returns:
        Index information as a string
    """
    await ctx.info(f"Listing indexes for table: {schema}.{table_name}")

    # Get or create connection manager
    manager = await get_or_create_manager(ctx)
    if not manager:
        return "Error: Failed to initialize database connection. Check configuration."

    query = """
    SELECT
        projection_name,
        is_super_projection,
        anchor_table_name
    FROM v_catalog.projections
    WHERE projection_schema = %s
    AND anchor_table_name = %s
    ORDER BY projection_name;
    """

    conn = None
    cursor = None
    try:
        conn = manager.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (schema, table_name))
        indexes = cursor.fetchall()

        if not indexes:
            return f"No projections found for table: {schema}.{table_name}"

        # Format the output for projections
        result = f"Projections for {schema}.{table_name}:\n\n"
        for proj in indexes:
            # proj[0]: projection_name, proj[1]: is_super_projection, proj[2]: anchor_table_name
            result += f"- {proj[0]} (Super Projection: {proj[1]}) [Table: {proj[2]}]\n"
        return result

    except Exception as e:
        error_msg = f"Error listing indexes: {str(e)}"
        await ctx.error(error_msg)
        return error_msg
    finally:
        if cursor:
            cursor.close()
        if conn:
            manager.release_connection(conn)


@mcp.tool()
async def list_views(
    ctx: Context,
    schema: str = "public"
) -> str:
    """List all views in a schema.

    Args:
        ctx: FastMCP context for progress reporting and logging
        schema: Schema name (default: public)

    Returns:
        View information as a string
    """
    await ctx.info(f"Listing views in schema: {schema}")

    # Get or create connection manager
    manager = await get_or_create_manager(ctx)
    if not manager:
        return "Error: Failed to initialize database connection. Check configuration."

    query = """
    SELECT
        table_name,
        view_definition
    FROM v_catalog.views
    WHERE table_schema = %s
    ORDER BY table_name;
    """

    conn = None
    cursor = None
    try:
        conn = manager.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (schema,))
        views = cursor.fetchall()

        if not views:
            return f"No views found in schema: {schema}"

        result = f"Views in schema {schema}:\n\n"
        for view in views:
            result += f"View: {view[0]}\n"
            result += f"Definition:\n{view[1]}\n\n"

        return result

    except Exception as e:
        error_msg = f"Error listing views: {str(e)}"
        await ctx.error(error_msg)
        return error_msg
    finally:
        if cursor:
            cursor.close()
        if conn:
            manager.release_connection(conn)
