import asyncio
import logging
import os
import click
from dotenv import load_dotenv
from .mcp import *
from .connection import (
    VERTICA_HOST,
    VERTICA_PORT,
    VERTICA_DATABASE,
    VERTICA_USER,
    VERTICA_PASSWORD,
    VERTICA_CONNECTION_LIMIT,
    VERTICA_SSL,
    VERTICA_SSL_REJECT_UNAUTHORIZED,
)

__version__ = "0.1.0"

def main(
    verbose: int,
    env_file: str | None,
    transport: str,
    port: int,
    host: str | None,
    db_port: int | None,
    database: str | None,
    user: str | None,
    password: str | None,
    connection_limit: int | None,
    ssl: bool | None,
    ssl_reject_unauthorized: bool | None,
) -> None:
    """MCP Vertica Server - Vertica functionality for MCP"""
    # Configure logging based on verbosity
    logging_level = logging.WARNING
    if verbose == 1:
        logging_level = logging.INFO
    elif verbose == 2:
        logging_level = logging.DEBUG
    elif verbose >= 3:
        logging_level = logging.DEBUG
        # Enable debug logging for all loggers
        for logger_name in ["mcp-vertica", "uvicorn", "starlette"]:
            logging.getLogger(logger_name).setLevel(logging.DEBUG)

    # Set up logging
    logging.basicConfig(
        level=logging_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Set default environment variables
    os.environ.setdefault(VERTICA_CONNECTION_LIMIT, "10")
    os.environ.setdefault(VERTICA_SSL, "false")
    os.environ.setdefault(VERTICA_SSL_REJECT_UNAUTHORIZED, "true")

    # Load environment variables from file if specified, otherwise try default .env
    if env_file:
        logging.debug(f"Loading environment from file: {env_file}")
        load_dotenv(env_file)
    else:
        logging.debug("Attempting to load environment from default .env file")
        load_dotenv()

    # Set environment variables from command line arguments if provided
    if host:
        os.environ[VERTICA_HOST] = host
    if db_port:
        os.environ[VERTICA_PORT] = str(db_port)
    if database:
        os.environ[VERTICA_DATABASE] = database
    if user:
        os.environ[VERTICA_USER] = user
    if password:
        os.environ[VERTICA_PASSWORD] = password
    if connection_limit:
        os.environ[VERTICA_CONNECTION_LIMIT] = str(connection_limit)
    if ssl is not None:
        os.environ[VERTICA_SSL] = str(ssl).lower()
    if ssl_reject_unauthorized is not None:
        os.environ[VERTICA_SSL_REJECT_UNAUTHORIZED] = str(ssl_reject_unauthorized).lower()

    # Run the server with specified transport
    if transport == "sse":
        asyncio.run(run_sse(port=port))
    else:
        mcp.run()

@click.command()
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Increase verbosity (can be used multiple times, e.g., -v, -vv, -vvv)",
)
@click.option(
    "--env-file", type=click.Path(exists=True, dir_okay=False), help="Path to .env file"
)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
    help="Transport type (stdio or sse)",
)
@click.option(
    "--port",
    default=8000,
    help="Port to listen on for SSE transport",
)
@click.option(
    "--host",
    help="Vertica host",
)
@click.option(
    "--db-port",
    type=int,
    help="Vertica port",
)
@click.option(
    "--database",
    help="Vertica database name",
)
@click.option(
    "--user",
    help="Vertica username",
)
@click.option(
    "--password",
    help="Vertica password",
)
@click.option(
    "--connection-limit",
    type=int,
    default=10,
    help="Maximum number of connections in the pool",
)
@click.option(
    "--ssl",
    is_flag=True,
    default=False,
    help="Enable SSL for database connection",
)
@click.option(
    "--ssl-reject-unauthorized",
    is_flag=True,
    default=True,
    help="Reject unauthorized SSL certificates",
)
def cli(**kwargs):
    main(**kwargs)

if __name__ == "__main__":
    cli()

__all__ = ["main", "cli"]
