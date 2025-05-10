# Vertica MCP

A MCP Server for Vertica database.

## Installation

```bash
uvx run mcp-vertica
```


## Supported Tools


## Running in Docker Environment

When running Vertica with Docker Compose, you can run the MCP server as follows:

### 1. Run with Direct Parameters

```bash
uv run mcp-vertica \
  --transport sse \
  --port 8000 \
  --host localhost \
  --db-port 5433 \
  --database test_db \
  --user test_user \
  --password test_password \
  --connection-limit 10
```

### 2. Run with Environment Variables

Create a `.env` file with the following content:

```env
VERTICA_HOST=localhost
VERTICA_PORT=5433
VERTICA_DATABASE=test_db
VERTICA_USER=test_user
VERTICA_PASSWORD=test_password
VERTICA_CONNECTION_LIMIT=10
VERTICA_SSL=false
VERTICA_SSL_REJECT_UNAUTHORIZED=true
```

Then run with .env 

```bash
uv run mcp-vertica \
  --env-file .env
```

### Docker Compose Example

```yaml
vertica:
  image: vertica/vertica-ce:latest
  environment:
    VERTICA_DB_NAME: test_db
    VERTICA_DB_USER: test_user
    VERTICA_DB_PASSWORD: test_password
  ports:
    - "5433:5433"
  volumes:
    - vertica_data:/home/dbadmin/test_db
  healthcheck:
    test: ["CMD", "vsql", "-U", "test_user", "-w", "test_password", "-d", "test_db", "-c", "SELECT 1"]
    interval: 5s
    timeout: 5s
    retries: 5
  restart: unless-stopped
