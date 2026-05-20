#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$PROJECT_DIR/bin"
PID_FILE="$BIN_DIR/socialserver.pid"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo -e "${GREEN}SocialServer is running${NC}, PID: $PID"
        exit 0
    fi
    echo -e "${YELLOW}PID file exists but process is not running${NC}"
    exit 1
fi

echo -e "${RED}SocialServer is not running${NC}"
exit 1