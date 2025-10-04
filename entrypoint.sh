#!/bin/sh
set -e

CMD="uv run mcp-vertica"

case "${DEBUG:-0}" in
    3)
        CMD="$CMD -vvv"
        ;;
    2)
        CMD="$CMD -vv"
        ;;
    1)
        CMD="$CMD -v"
        ;;
esac

if [ -n "$EXTRA_ARGS" ]; then
    CMD="$CMD $EXTRA_ARGS"
fi

echo "Executing: $CMD"
exec $CMD
