import warnings
import re

warnings.filterwarnings(
    "ignore",
    message='Field name "schema" in ".*" shadows an attribute in parent "ArgModelBase"',
    category=UserWarning,
    module="pydantic._internal._fields",
)

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

__version__ = "0.1.6"

logger = logging.getLogger("mcp-vertica")


def setup_logger(verbose: int) -> logging.Logger:
    logger = logging.getLogger("mcp-vertica")
    logger.propagate = False
    level = logging.CRITICAL
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        if verbose == 0:
            handler.setLevel(logging.CRITICAL)
            logger.setLevel(logging.CRITICAL)
        elif verbose == 1:
            handler.setLevel(logging.INFO)
            logger.setLevel(logging.INFO)
            level = logging.INFO
        else:
            handler.setLevel(logging.DEBUG)
            logger.setLevel(logging.DEBUG)
            level = logging.DEBUG
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logging.basicConfig(level=level, force=True)
    return logger


def validate_port(ctx, param, value):
    if value is not None and not (1024 <= value <= 65535):
        raise click.BadParameter(f"{param.name} must be between 1024 and 65535")
    return value


def validate_host(ctx, param, value):
    if value is not None:
        if not re.match(r"^[\w\.-]+$", value):
            raise click.BadParameter(
                f"{param.name} must be a valid hostname or IP address"
            )
    return value


def main(
    verbose: int,
    env_file: str | None,
    transport: str | None,
    port: int | None,
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
    setup_logger(verbose)

    # Determine transport mode: CLI option > TRANSPORT env var > default (http)
    if transport is None:
        transport = os.environ.get("TRANSPORT", "http")
    logger.info(f"Using transport mode: {transport}")

    # Determine port: CLI option > PORT env var > default (8081)
    if port is None:
        port = int(os.environ.get("PORT", "8081"))
    logger.info(f"Using port: {port}")

    # Set default environment variables
    os.environ.setdefault(VERTICA_CONNECTION_LIMIT, "10")
    os.environ.setdefault(VERTICA_SSL, "false")
    os.environ.setdefault(VERTICA_SSL_REJECT_UNAUTHORIZED, "true")

    # Priority order for configuration:
    # 1. Command line arguments (highest priority)
    # 2. Environment variables (Docker/Smithery sets these)
    # 3. .env file
    # 4. Default values (lowest priority)

    # Load environment variables from file if specified, otherwise try default .env
    # This will NOT override existing environment variables
    if env_file:
        logging.debug(f"Loading environment from file: {env_file}")
        load_dotenv(env_file, override=False)
    else:
        logging.debug("Attempting to load environment from default .env file")
        load_dotenv(override=False)

    # Override with command line arguments if provided (CLI has highest priority)
    if host is not None:
        os.environ[VERTICA_HOST] = host
    if db_port is not None:
        os.environ[VERTICA_PORT] = str(db_port)
    if database is not None:
        os.environ[VERTICA_DATABASE] = database
    if user is not None:
        os.environ[VERTICA_USER] = user
    if password is not None:
        os.environ[VERTICA_PASSWORD] = password
    if connection_limit is not None:
        os.environ[VERTICA_CONNECTION_LIMIT] = str(connection_limit)
    if ssl is not None:
        os.environ[VERTICA_SSL] = str(ssl).lower()
    if ssl_reject_unauthorized is not None:
        os.environ[VERTICA_SSL_REJECT_UNAUTHORIZED] = str(
            ssl_reject_unauthorized
        ).lower()

    if not password:
        logging.warning("Empty password should be used only in local enviroment")
        print("Empty password should be used only in local enviroment")

    # Run the server with specified transport
    if transport == "sse":
        asyncio.run(run_sse(port=port))
    elif transport == "http":
        run_http(port=port)
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
    type=click.Choice(["stdio", "sse", "http"]),
    default=None,
    help="Transport type (stdio, sse, or http). If not specified, uses TRANSPORT env var or defaults to http",
)
@click.option(
    "--port",
    type=int,
    default=None,
    callback=validate_port,
    help="Port to listen on for SSE/HTTP transport. If not specified, uses PORT env var or defaults to 8081",
)
@click.option(
    "--host",
    callback=validate_host,
    help="Vertica host",
)
@click.option(
    "--db-port",
    type=int,
    callback=validate_port,
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
    required=False,
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
