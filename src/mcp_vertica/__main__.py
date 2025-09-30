"""Entry point for running mcp_vertica as a module."""
from . import cli

if __name__ == "__main__":
    # When run as a module (python -m mcp_vertica), use CLI
    cli()
