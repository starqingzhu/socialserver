#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}SocialServer build tool${NC}"
echo -e "${GREEN}========================================${NC}"

echo -e "${BLUE}Select environment (20秒内选择，超时默认本地环境):${NC}"
echo "1) 本地开发环境 (.devops.yaml)"
echo "2) 测试环境 (.devops_test.yaml)"
echo "3) 生产环境 (.devops_production.yaml)"
echo "4) 提审环境 (.devops_inter.yaml)"

if read -t 20 -p "Choose [1-4, 默认1]: " env_choice; then
    echo ""
else
    echo ""
    echo -e "${YELLOW}Timeout, using default local environment${NC}"
    env_choice=1
fi

case ${env_choice:-1} in
    1)
        CONFIG_FILE=".devops.yaml"
        ENV_NAME="local"
        ;;
    2)
        CONFIG_FILE=".devops_test.yaml"
        ENV_NAME="test"
        ;;
    3)
        CONFIG_FILE=".devops_production.yaml"
        ENV_NAME="production"
        ;;
    4)
        CONFIG_FILE=".devops_inter.yaml"
        ENV_NAME="提审"
        ;;
    *)
        echo -e "${YELLOW}Invalid option, using default local environment${NC}"
        CONFIG_FILE=".devops.yaml"
        ENV_NAME="local"
        ;;
esac

mkdir -p ../bin

OS=$(uname -s)
ARCH=$(uname -m)

case $ARCH in
    x86_64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    armv7l) ARCH="arm" ;;
    i386|i686) ARCH="386" ;;
esac

case $OS in
    Linux*)
        GOOS="linux"
        OUTPUT="../bin/socialserver"
        ;;
    Darwin*)
        GOOS="darwin"
        OUTPUT="../bin/socialserver"
        ;;
    CYGWIN*|MINGW*|MSYS*)
        GOOS="windows"
        OUTPUT="../bin/socialserver.exe"
        ;;
    *)
        GOOS="linux"
        OUTPUT="../bin/socialserver"
        ;;
esac

GIT_COMMIT_SHA1=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "Building for $GOOS/$ARCH"
GOOS=$GOOS GOARCH=$ARCH go build \
    -ldflags "-X 'main.gitCommitSha1=$GIT_COMMIT_SHA1' -X 'main.date=$BUILD_DATE'" \
    -o $OUTPUT \
    -v ../cmd/main.go

echo ""
echo -e "${YELLOW}Copying config...${NC}"
if [ -f "../conf/${CONFIG_FILE}" ]; then
    cp -f "../conf/${CONFIG_FILE}" "../bin/.devops.yaml"
    echo "Copied ${CONFIG_FILE} -> bin/.devops.yaml"
else
    echo -e "${RED}Config file ../conf/${CONFIG_FILE} not found${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}Build complete${NC}"
echo "Environment: ${ENV_NAME}"
echo "Binary: ${OUTPUT}"