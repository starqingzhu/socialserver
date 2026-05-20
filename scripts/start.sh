#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$PROJECT_DIR/bin"
LOGS_DIR="$PROJECT_DIR/logs"
PID_FILE="$BIN_DIR/socialserver.pid"
EXEC_FILE="$BIN_DIR/socialserver"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

pre_start_check() {
    if [ ! -f "$EXEC_FILE" ]; then
        log_error "Binary not found: $EXEC_FILE"
        log_info "Run ./scripts/build.sh first"
        exit 1
    fi

    chmod +x "$EXEC_FILE"

    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        log_warn "SocialServer already running, PID: $(cat $PID_FILE)"
        exit 1
    fi

    mkdir -p "$LOGS_DIR"
    mkdir -p "$BIN_DIR"

    if [ ! -f "$BIN_DIR/.devops.yaml" ]; then
        log_error "Config file not found: $BIN_DIR/.devops.yaml"
        log_info "Run ./scripts/build.sh first"
        exit 1
    fi
}

start_server() {
    log_info "Starting SocialServer..."
    cd "$BIN_DIR"
    nohup "$EXEC_FILE" > /dev/null 2>&1 &
    local pid=$!
    echo $pid > "$PID_FILE"

    sleep 3
    if kill -0 $pid 2>/dev/null; then
        log_info "SocialServer started successfully"
        log_info "PID: $pid"
        log_info "Logs: $LOGS_DIR/"
    else
        log_error "SocialServer failed to start"
        rm -f "$PID_FILE"
        exit 1
    fi
}

main() {
    log_info "=== Start SocialServer ==="
    pre_start_check
    start_server
    log_info "=== Done ==="
}

main "$@"