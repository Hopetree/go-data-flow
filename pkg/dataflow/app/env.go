// Package app 提供应用程序框架。开发者只需关注组件注册
package app

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// envEscapePlaceholder 用于 $$ 转义的临时占位符（YAML 内容中不应出现 null 字节）
const envEscapePlaceholder = "\x00"

// LoadEnv 加载 .env 文件到环境变量
// 文件不存在时静默跳过，仅在其他错误（如权限问题）时返回错误
func LoadEnv(envFile string) error {
	return godotenv.Load(envFile)
}

// expandEnvVars 对配置内容执行 ${VAR} 环境变量替换
// 使用 $$ 表示字面量 $，避免被误解析为环境变量引用
func expandEnvVars(data []byte) []byte {
	s := string(data)
	// $$ → 占位符
	s = strings.ReplaceAll(s, "$$", envEscapePlaceholder)
	// 执行环境变量替换
	s = os.ExpandEnv(s)
	// 占位符 → $
	s = strings.ReplaceAll(s, envEscapePlaceholder, "$")
	return []byte(s)
}
