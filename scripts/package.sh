#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PROJECT_NAME="socialserver"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}SocialServer package tool${NC}"
echo -e "${GREEN}========================================${NC}"

echo -e "${BLUE}Select target platform (default linux amd64):${NC}"
echo "1) Linux (amd64)"
echo "2) Linux (arm64)"
echo "3) Windows (amd64)"
echo "4) macOS (amd64)"
echo "5) macOS (arm64)"

if read -t 60 -p "Choose [1-5, default 1]: " platform_choice; then
    echo ""
else
    echo ""
    echo -e "${YELLOW}Timeout, using default linux amd64${NC}"
    platform_choice=1
fi

case ${platform_choice:-1} in
    1) TARGET_OS="linux"; TARGET_ARCH="amd64"; BINARY_NAME="socialserver" ;;
    2) TARGET_OS="linux"; TARGET_ARCH="arm64"; BINARY_NAME="socialserver" ;;
    3) TARGET_OS="windows"; TARGET_ARCH="amd64"; BINARY_NAME="socialserver.exe" ;;
    4) TARGET_OS="darwin"; TARGET_ARCH="amd64"; BINARY_NAME="socialserver" ;;
    5) TARGET_OS="darwin"; TARGET_ARCH="arm64"; BINARY_NAME="socialserver" ;;
    *) TARGET_OS="linux"; TARGET_ARCH="amd64"; BINARY_NAME="socialserver" ;;
esac

echo ""
echo -e "${BLUE}Select environment (default local):${NC}"
echo "1) local (.devops.yaml)"
echo "2) test (.devops_test.yaml)"
echo "3) production (.devops_production.yaml)"

if read -t 60 -p "Choose [1-3, default 1]: " env_choice; then
    echo ""
else
    echo ""
    echo -e "${YELLOW}Timeout, using default local environment${NC}"
    env_choice=1
fi

case ${env_choice:-1} in
    1) CONFIG_FILE=".devops.yaml"; ENV_NAME="local" ;;
    2) CONFIG_FILE=".devops_test.yaml"; ENV_NAME="test" ;;
    3) CONFIG_FILE=".devops_production.yaml"; ENV_NAME="production" ;;
    *) CONFIG_FILE=".devops.yaml"; ENV_NAME="local" ;;
esac

VERSION=${1:-$(date +"%Y%m%d_%H%M%S")}
PACKAGE_DIR="../release"
TEMP_DIR="${PACKAGE_DIR}/${PROJECT_NAME}_${VERSION}"

mkdir -p "${PACKAGE_DIR}"
find "${PACKAGE_DIR}" -maxdepth 1 -name "${PROJECT_NAME}_*" -type d -exec rm -rf {} + 2>/dev/null || true
mkdir -p "${TEMP_DIR}/bin" "${TEMP_DIR}/logs" "${TEMP_DIR}/scripts"

cd "$(dirname "$0")"

GOOS=$TARGET_OS GOARCH=$TARGET_ARCH go build -ldflags="-s -w" -o "${TEMP_DIR}/bin/${BINARY_NAME}" ../cmd/main.go

if [ -f "../conf/${CONFIG_FILE}" ]; then
    cp -f "../conf/${CONFIG_FILE}" "${TEMP_DIR}/bin/.devops.yaml"
else
    echo -e "${RED}Config file ${CONFIG_FILE} not found${NC}"
    exit 1
fi

if [ -d "../config" ]; then
    mkdir -p "${TEMP_DIR}/config"
    cp -r ../config/* "${TEMP_DIR}/config/" 2>/dev/null || true
fi

cp -f start.sh stop.sh restart.sh status.sh "${TEMP_DIR}/scripts/"

if [ -f "../README.md" ]; then
    cp -f "../README.md" "${TEMP_DIR}/README.md"
fi

echo ""
echo -e "${GREEN}Package complete${NC}"
echo "Version: ${VERSION}"
echo "Platform: ${TARGET_OS}/${TARGET_ARCH}"
echo "Environment: ${ENV_NAME}"
echo "Output: ${TEMP_DIR}"