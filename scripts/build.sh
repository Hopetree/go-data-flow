#!/bin/bash
#
# build.sh - 跨平台打包脚本
#
# 用法:
#   ./scripts/build.sh [选项]
#
# 选项:
#   -p, --platform    目标平台: mac, linux, windows, all (默认: all)
#   -a, --arch        目标架构: amd64, arm64 (默认: amd64)
#   -v, --version     版本号 (默认: 从 git tag 获取或使用 dev)
#   -o, --output      输出目录 (默认: ./dist)
#   -h, --help        显示帮助信息
#
# 示例:
#   ./scripts/build.sh -p mac -a arm64 -v $(date +'%y.%m.%d') -o ./dist
#   ./scripts/build.sh -p linux -a amd64 -v $(date +'%y.%m.%d') -o ./dist
#   ./scripts/build.sh -p linux -v 1.0.0
#   ./scripts/build.sh --platform mac
#   ./scripts/build.sh -p all
#

set -e

# 默认值
PLATFORM="all"
ARCH="amd64"
VERSION=""
OUTPUT_DIR="./dist"
BINARY_NAME="dataflow"
MAIN_PATH="github.com/Hopetree/go-data-flow/cmd/dataflow"
BUILD_TIME=$(date "+%Y-%m-%d %H:%M:%S")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# 打印函数
info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error_exit() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# 获取版本号
get_version() {
    if [[ -z "$VERSION" ]]; then
        VERSION=$(git describe --tags 2>/dev/null || echo "dev")
    fi
    info "版本: ${VERSION}"
}

# 清理输出目录
clean_output() {
    info "清理输出目录: ${OUTPUT_DIR}"
    mkdir -p "${OUTPUT_DIR}"
    find "${OUTPUT_DIR}" -mindepth 1 ! -name '.gitkeep' -delete
}

# 构建单个平台
build_platform() {
    local os=$1
    local arch=$2
    local output_name="${BINARY_NAME}-${os}-${arch}"
    if [[ "$os" == "windows" ]]; then
        output_name="${BINARY_NAME}-${os}-${arch}.exe"
    fi

    info "构建 ${os}/${arch}..."

    LD_FLAGS="-s -w -X 'main.Version=${VERSION}' -X 'main.BuildTime=${BUILD_TIME}' -X 'main.GitCommit=${GIT_COMMIT}'"

    GOOS=$os GOARCH=$arch CGO_ENABLED=0 \
        go build -ldflags "${LD_FLAGS}" \
        -o "${OUTPUT_DIR}/${output_name}" \
        "${MAIN_PATH}"

    if [[ $? -ne 0 ]]; then
        error_exit "构建失败: ${os}/${arch}"
    fi

    success "生成: ${output_name}"
}

# 构建所有平台
build_all() {
    info "开始构建所有平台..."
    build_platform "darwin" "amd64"
    build_platform "linux" "amd64"
    build_platform "windows" "amd64"
}

# 显示帮助
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -p, --platform    目标平台: mac, linux, windows, all (默认: all)"
    echo "  -a, --arch        目标架构: amd64, arm64 (默认: amd64)"
    echo "  -v, --version     版本号 (默认: dev)"
    echo "  -o, --output      输出目录 (默认: ./dist)"
    echo "  -h, --help        显示帮助信息"
    exit 0
}

# 显示结果
show_result() {
    echo ""
    info "构建完成! 输出文件:"
    ls -lh "${OUTPUT_DIR}"
    echo ""
}

# 解析参数
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -p|--platform)
                shift
                PLATFORM="$1"
                shift
                ;;
            -a|--arch)
                shift
                ARCH="$1"
                shift
                ;;
            -v|--version)
                shift
                VERSION="$1"
                shift
                ;;
            -o|--output)
                shift
                OUTPUT_DIR="$1"
                shift
                ;;
            -h|--help)
                show_help
                ;;
            *)
                echo "未知选项: $1"
                exit 1
                ;;
        esac
    done
}

# 主流程
main() {
    parse_args "$@"
    get_version
    clean_output

    case "$PLATFORM" in
        all)
            build_all
            ;;
        mac|darwin)
            build_platform "darwin" "${ARCH}"
            ;;
        linux)
            build_platform "linux" "${ARCH}"
            ;;
        windows)
            build_platform "windows" "${ARCH}"
            ;;
        *)
            echo "不支持的平台: ${PLATFORM}"
            exit 1
            ;;
    esac

    show_result
}

main "$@"
