from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from mcp.server.fastmcp import FastMCP, Context
from typing import Any, AsyncGenerator, List
import logging
import re
from .connection import VerticaConnectionManager, VerticaConfig, OperationType
from starlette.applications import Starlette
from starlette.routing import Mount
import uvicorn
import csv
import io

# Configure logging
logger = logging.getLogger("mcp-vertica")


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
        Dictionary containing the Vertica connection manager
    """
    manager = None
    try:
        # Initialize Vertica connection manager
        manager = VerticaConnectionManager()
        config = VerticaConfig.from_env()
        manager.initialize_default(config)
        logger.info("Vertica connection manager initialized")
        yield {"vertica_manager": manager}
    except Exception as e:
        logger.error(f"Failed to initialize server: {str(e)}")
        raise
    finally:
        # Cleanup resources
        if manager:
            try:
                manager.close_all()
                logger.info("Vertica connection manager closed")
            except Exception as e:
                logger.error(f"Error during cleanup: {str(e)}")


# Create FastMCP instance with SSE support
mcp = FastMCP(
    "Vertica Service",
    dependencies=["vertica-python", "pydantic", "starlette", "uvicorn"],
    lifespan=server_lifespan,
)


async def run_sse(port: int = 8000) -> None:
    """Run the MCP server with SSE transport.

    Args:
        port: Port to listen on for SSE transport
    """
    starlette_app = Starlette(routes=[Mount("/", app=mcp.sse_app())])
    config = uvicorn.Config(starlette_app, host="0.0.0.0", port=port)  # noqa: S104
    app = uvicorn.Server(config)
    await app.serve()


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

    # Get connection manager from context
    manager = ctx.request_context.lifespan_context.get("vertica_manager")
    if not manager:
        await ctx.error("No database connection manager available")
        return "Error: No database connection manager available"

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
) -> AsyncGenerator[str, None]:
    """Execute a SQL query and stream the results in batches.

    Args:
        ctx: FastMCP context for progress reporting and logging
        query: SQL query to execute
        batch_size: Number of rows to fetch at once

    Yields:
        Batches of query results as strings
    """
    await ctx.info(f"Streaming query: {query}")

    # Get connection manager from context
    manager = ctx.request_context.lifespan_context.get("vertica_manager")
    if not manager:
        await ctx.error("No database connection manager available")
        yield "Error: No database connection manager available"
        return

    # Extract schema from query if not provided
    schema = extract_schema_from_query(query)
    # Check operation permissions
    operation = extract_operation_type(query)
    if operation and not manager.is_operation_allowed(schema or "default", operation):
        error_msg = f"Operation {operation.name} not allowed for schema {schema}"
        await ctx.error(error_msg)
        yield error_msg
        return

    conn = None
    cursor = None
    try:
        conn = manager.get_connection()  # Always use default DB connection
        cursor = conn.cursor()
        cursor.execute(query)

        total_rows = 0
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            total_rows += len(batch)
            await ctx.debug(f"Fetched {total_rows} rows")
            yield str(batch)

        await ctx.info(f"Query streaming completed, total rows: {total_rows}")
    except Exception as e:
        error_msg = f"Error streaming query: {str(e)}"
        await ctx.error(error_msg)
        yield error_msg
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

    # Get connection manager from context
    manager = ctx.request_context.lifespan_context.get("vertica_manager")
    if not manager:
        await ctx.error("No database connection manager available")
        return "Error: No database connection manager available"

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

    manager = ctx.request_context.lifespan_context.get("vertica_manager")
    if not manager:
        await ctx.error("No database connection manager available")
        return "Error: No database connection manager available"

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

    manager = ctx.request_context.lifespan_context.get("vertica_manager")
    if not manager:
        await ctx.error("No database connection manager available")
        return "Error: No database connection manager available"

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

    manager = ctx.request_context.lifespan_context.get("vertica_manager")
    if not manager:
        await ctx.error("No database connection manager available")
        return "Error: No database connection manager available"

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
