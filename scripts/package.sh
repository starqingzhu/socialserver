#!/bin/bash
# filepath: socialserver/scripts/package.sh
# 打包脚本：编译并打包服务器程序及配置文件

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目名称
PROJECT_NAME="socialserver"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}社交服务器打包工具${NC}"
echo -e "${GREEN}========================================${NC}"

# 1. 选择编译平台（60秒超时，默认Linux amd64）
echo -e "${BLUE}请选择编译平台（60秒内选择，超时默认Linux amd64）:${NC}"
echo "1) Linux (amd64)"
echo "2) Linux (arm64)"
echo "3) Windows (amd64)"
echo "4) macOS (amd64)"
echo "5) macOS (arm64)"

if read -t 60 -p "请输入选项 [1-5, 默认1]: " platform_choice; then
    echo ""
else
    echo ""
    echo -e "${YELLOW}超时，使用默认: Linux (amd64)${NC}"
    platform_choice=1
fi

case ${platform_choice:-1} in
    1)
        TARGET_OS="linux"
        TARGET_ARCH="amd64"
        BINARY_NAME="socialserver"
        ;;
    2)
        TARGET_OS="linux"
        TARGET_ARCH="arm64"
        BINARY_NAME="socialserver"
        ;;
    3)
        TARGET_OS="windows"
        TARGET_ARCH="amd64"
        BINARY_NAME="socialserver.exe"
        ;;
    4)
        TARGET_OS="darwin"
        TARGET_ARCH="amd64"
        BINARY_NAME="socialserver"
        ;;
    5)
        TARGET_OS="darwin"
        TARGET_ARCH="arm64"
        BINARY_NAME="socialserver"
        ;;
    *)
        echo -e "${YELLOW}无效选项，使用默认: Linux (amd64)${NC}"
        TARGET_OS="linux"
        TARGET_ARCH="amd64"
        BINARY_NAME="socialserver"
        ;;
esac

# 2. 选择部署环境（60秒超时，默认本地环境）
echo ""
echo -e "${BLUE}请选择部署环境（60秒内选择，超时默认本地环境）:${NC}"
echo "1) 本地开发环境 (.devops.yaml)"
echo "2) 测试环境 (.devops_test.yaml)"
echo "3) 生产环境 (.devops_production.yaml)"
echo "4) 提审环境 (.devops_inter.yaml)"

if read -t 60 -p "请输入选项 [1-3, 默认1]: " env_choice; then
    echo ""
else
    echo ""
    echo -e "${YELLOW}超时，使用默认: 本地开发环境${NC}"
    env_choice=1
fi

case ${env_choice:-1} in
    1)
        CONFIG_FILE=".devops.yaml"
        ENV_NAME="本地开发"
        ;;
    2)
        CONFIG_FILE=".devops_test.yaml"
        ENV_NAME="测试"
        ;;
    3)
        CONFIG_FILE=".devops_production.yaml"
        ENV_NAME="生产"
        ;;
    4)
        CONFIG_FILE=".devops_inter.yaml"
        ENV_NAME="提审"
        ;;
    *)
        echo -e "${YELLOW}无效选项，使用默认: 本地开发环境${NC}"
        CONFIG_FILE=".devops.yaml"
        ENV_NAME="本地开发"
        ;;
esac

# 版本号（可以从参数传入，默认使用时间戳）
VERSION=${1:-$(date +"%Y%m%d_%H%M%S")}
# 打包输出目录
PACKAGE_DIR="../release"
# 临时打包目录
TEMP_DIR="${PACKAGE_DIR}/${PROJECT_NAME}_${VERSION}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}打包配置:${NC}"
echo -e "  项目: ${PROJECT_NAME}"
echo -e "  版本: ${VERSION}"
echo -e "  平台: ${TARGET_OS}/${TARGET_ARCH}"
echo -e "  环境: ${ENV_NAME}"
echo -e "  配置: ${CONFIG_FILE}"
echo -e "${GREEN}========================================${NC}"

# 1. 清理并创建目录
echo ""
echo -e "${YELLOW}[1/7] 准备打包环境...${NC}"

mkdir -p "${PACKAGE_DIR}"
find "${PACKAGE_DIR}" -maxdepth 1 -name "${PROJECT_NAME}_*" -type d -exec rm -rf {} + 2>/dev/null || true

mkdir -p "${TEMP_DIR}/bin"
mkdir -p "${TEMP_DIR}/conf"
mkdir -p "${TEMP_DIR}/config"
mkdir -p "${TEMP_DIR}/logs"
mkdir -p "${TEMP_DIR}/scripts"

# 2. 编译程序
echo -e "${YELLOW}[2/7] 编译程序...${NC}"
cd "$(dirname "$0")"

echo "目标平台: $TARGET_OS/$TARGET_ARCH"
GOOS=$TARGET_OS GOARCH=$TARGET_ARCH go build -ldflags="-s -w" -o "${TEMP_DIR}/bin/${BINARY_NAME}" ../cmd/main.go

if [ $? -ne 0 ]; then
    echo -e "${RED}编译失败！${NC}"
    exit 1
fi

echo -e "${GREEN}编译成功: ${BINARY_NAME}${NC}"

# 3. 复制配置文件
echo -e "${YELLOW}[3/7] 复制配置文件...${NC}"

# 复制选定的 devops 配置文件到 bin 目录
if [ -f "../conf/${CONFIG_FILE}" ]; then
    cp -f "../conf/${CONFIG_FILE}" "${TEMP_DIR}/bin/.devops.yaml"
    echo "已复制: ${CONFIG_FILE} -> bin/.devops.yaml"
else
    echo -e "${RED}错误: 配置文件 ${CONFIG_FILE} 不存在！${NC}"
    exit 1
fi

# 复制 TLS 证书和密钥
if [ -d "../conf" ]; then
    case ${env_choice:-1} in
        1)
            # 本地开发环境使用 server.crt/server.key
            if [ -f "../conf/server.crt" ]; then
                cp -f "../conf/server.crt" "${TEMP_DIR}/conf/"
                echo "已复制: server.crt -> conf/"
            fi
            if [ -f "../conf/server.key" ]; then
                cp -f "../conf/server.key" "${TEMP_DIR}/conf/"
                echo "已复制: server.key -> conf/"
            fi
            ;;
        2|3|4)
            # 测试/生产环境使用正式域名证书
            if [ -f "../conf/_.gamescombine.com_with_chain.crt" ]; then
                cp -f "../conf/_.gamescombine.com_with_chain.crt" "${TEMP_DIR}/conf/"
                echo "已复制: _.gamescombine.com_with_chain.crt -> conf/"
            else
                echo -e "${YELLOW}警告: 证书文件 _.gamescombine.com_with_chain.crt 不存在${NC}"
            fi
            if [ -f "../conf/_.gamescombine.com.key" ]; then
                cp -f "../conf/_.gamescombine.com.key" "${TEMP_DIR}/conf/"
                echo "已复制: _.gamescombine.com.key -> conf/"
            else
                echo -e "${YELLOW}警告: 私钥文件 _.gamescombine.com.key 不存在${NC}"
            fi
            ;;
    esac
fi

# 复制 config 目录（业务配置：RankBase.json、RobotName.json、RobotRank.json 等）
if [ -d "../config" ]; then
    cp -r ../config/* "${TEMP_DIR}/config/" 2>/dev/null || true
    echo "已复制: config/ 目录（业务配置）"
fi

# 4. 复制脚本文件
echo -e "${YELLOW}[4/7] 复制脚本文件...${NC}"
cp -f start.sh stop.sh restart.sh status.sh "${TEMP_DIR}/scripts/"
echo "已复制: 启动/停止脚本"

# 5. 生成 README
echo -e "${YELLOW}[5/7] 生成 README...${NC}"
cat > "${TEMP_DIR}/README.md" << 'EOF'
# SocialServer 部署说明

## 目录结构
```
socialserver/
├── bin/                    # 可执行文件目录
│   ├── socialserver       # 服务器主程序
│   └── .devops.yaml       # 服务配置文件（已根据环境选择）
├── conf/                   # 系统配置目录（TLS证书）
│   ├── server.crt         # TLS证书文件（本地环境）
│   ├── server.key         # TLS私钥文件（本地环境）
│   ├── _.gamescombine.com_with_chain.crt  # TLS证书文件（测试/生产环境）
│   └── _.gamescombine.com.key             # TLS私钥文件（测试/生产环境）
├── config/                 # 业务配置目录
│   ├── RankBase.json      # 排行榜基础配置
│   ├── RobotName.json     # 机器人名称配置
│   └── RobotRank.json     # 机器人排名配置
├── logs/                   # 日志目录
└── scripts/                # 脚本目录
    ├── start.sh           # 启动脚本
    ├── stop.sh            # 停止脚本
    ├── restart.sh         # 重启脚本
    └── status.sh          # 状态查看脚本
```

## 部署步骤

### 1. 上传文件
将整个 socialserver 目录上传到服务器

### 2. 配置修改
根据实际环境修改配置文件：
```bash
vi bin/.devops.yaml
```

主要配置项：
- `lbs.uri`: LBS服务地址
- `redis`: Redis连接信息
- `mongodb`: MongoDB连接信息
- `rpc.listen_addr`: RPC监听地址
- `etcd`: Etcd连接信息

### 3. 赋予执行权限
```bash
chmod +x bin/socialserver
chmod +x scripts/*.sh
```

### 4. 启动服务
```bash
cd scripts
./start.sh
```

**注意**: 启动前确保 Redis、MongoDB、Etcd 等依赖服务正常运行

### 5. 查看状态
```bash
./status.sh
```

### 6. 查看日志
```bash
tail -f ../logs/socialserver.log.*
```

## 常用命令

### 启动服务
```bash
cd scripts && ./start.sh
```

### 停止服务
```bash
cd scripts && ./stop.sh
```

### 重启服务
```bash
cd scripts && ./restart.sh
```

### 查看状态
```bash
cd scripts && ./status.sh
```

### 查看实时日志
```bash
tail -f logs/socialserver.log.$(date +%Y%m%d)
```

## 性能分析（如果启用了 pprof）

```bash
# CPU Profile
curl http://localhost:9901/debug/pprof/profile?seconds=30 -o cpu.prof

# Heap Profile
curl http://localhost:9901/debug/pprof/heap -o heap.prof

# Goroutine Profile
curl http://localhost:9901/debug/pprof/goroutine -o goroutine.prof
```

分析 profile：
```bash
go tool pprof cpu.prof
```

## 注意事项

1. **端口检查**：确保配置的端口未被占用
2. **权限检查**：确保有足够的文件读写权限
3. **依赖服务**：确保 Redis、MongoDB、Etcd 等依赖服务正常运行
4. **日志轮转**：定期清理日志文件，避免磁盘占满

## 故障排查

### 1. 启动失败
- 检查配置文件格式是否正确
- 检查端口是否被占用
- 查看日志文件获取详细错误信息

### 2. 连接数据库失败
- 检查 Redis/MongoDB 地址和端口
- 检查用户名密码是否正确
- 检查网络连接是否正常

### 3. RPC服务注册失败
- 检查 Etcd 连接配置
- 检查服务名称和路径配置
- 查看日志获取详细错误

### 4. 性能问题
- 检查 MongoDB 连接池配置
- 查看 pprof 性能分析
- 检查日志级别设置

## 联系方式

如有问题，请联系运维团队。
EOF

echo "已生成: README.md"

# 6. 生成版本信息
echo -e "${YELLOW}[6/7] 生成版本信息...${NC}"
cat > "${TEMP_DIR}/VERSION" << EOF
Project: ${PROJECT_NAME}
Version: ${VERSION}
Build Time: $(date '+%Y-%m-%d %H:%M:%S')
Build OS: ${TARGET_OS}
Build Arch: ${TARGET_ARCH}
Git Branch: $(git branch --show-current 2>/dev/null || echo "unknown")
Git Commit: $(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
EOF

echo "已生成: VERSION"

# 7. 打包压缩
echo -e "${YELLOW}[7/7] 打包压缩...${NC}"

PACKAGE_NAME="${PROJECT_NAME}_${VERSION}_${TARGET_OS}_${TARGET_ARCH}.tar.gz"
PACKAGE_PATH="${PACKAGE_DIR}/${PACKAGE_NAME}"

cd "${PACKAGE_DIR}"
tar -czf "${PACKAGE_NAME}" "$(basename ${TEMP_DIR})"

if [ $? -ne 0 ]; then
    echo -e "${RED}打包失败！${NC}"
    exit 1
fi

# # 清理临时目录
# rm -rf "${TEMP_DIR}"

# 8. 输出结果
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}打包完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "包名称: ${GREEN}${PACKAGE_NAME}${NC}"
echo -e "包路径: ${GREEN}$(cd "${PACKAGE_DIR}" && pwd)/${PACKAGE_NAME}${NC}"
echo -e "包大小: ${GREEN}$(du -h "${PACKAGE_PATH}" | cut -f1)${NC}"
echo ""
echo -e "${YELLOW}部署方式：${NC}"
echo "1. 上传压缩包到服务器"
echo "2. 解压: tar -xzf ${PACKAGE_NAME}"
echo "3. 进入目录: cd $(basename ${TEMP_DIR})"
echo "4. 修改配置: vi bin/.devops.yaml"
echo "5. 赋予权限: chmod +x bin/socialserver scripts/*.sh"
echo "6. 启动服务: cd scripts && ./start.sh"
echo ""
echo -e "${GREEN}打包完成！${NC}"
