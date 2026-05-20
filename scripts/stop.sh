#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$PROJECT_DIR/bin"
PID_FILE="$BIN_DIR/socialserver.pid"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

if [ ! -f "$PID_FILE" ]; then
    log_warn "PID file not found"
    exit 0
fi

PID=$(cat "$PID_FILE")
if kill -0 "$PID" 2>/dev/null; then
    log_info "Stopping SocialServer, PID: $PID"
    kill "$PID"
    sleep 2
    if kill -0 "$PID" 2>/dev/null; then
        log_warn "Process still running, force killing"
        kill -9 "$PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    log_info "SocialServer stopped"
else
    log_warn "Process not running, removing stale PID file"
    rm -f "$PID_FILE"
fi