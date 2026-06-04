#!/bin/bash
# Agent Group Chat - 一键启动
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# 创建venv（如果不存在）
if [ ! -d ".venv" ]; then
    echo "Creating venv..."
    python3 -m venv .venv
    .venv/bin/pip install -q -r requirements.txt
    echo "Setup complete."
fi

# 默认配置（可通过环境变量覆盖）
export HERMES_API_URL="${HERMES_API_URL:-http://127.0.0.1:8642}"
export HERMES_API_KEY="${HERMES_API_KEY:-${API_SERVER_KEY:-local-chat}}"
export CHAT_SERVER_PORT="${CHAT_SERVER_PORT:-8080}"

# 检查 Hermes API Server
if curl -s --connect-timeout 2 "$HERMES_API_URL/health" > /dev/null 2>&1; then
    echo "✓ Hermes API Server is running at $HERMES_API_URL"
else
    echo "⚠ Hermes API Server not reachable at $HERMES_API_URL"
    echo "  Start it with:"
    echo "    hermes gateway restart"
    echo ""
fi

echo "Starting Agent Group Chat on http://localhost:$CHAT_SERVER_PORT"
exec .venv/bin/python server.py
