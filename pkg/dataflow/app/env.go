// Package app 提供应用程序框架。开发者只需关注组件注册
package app

import (
	"os"

	"github.com/joho/godotenv"
)

// LoadEnv 加载 .env 文件到环境变量
// 文件不存在时静默跳过，仅在其他错误（如权限问题）时返回错误
func LoadEnv(envFile string) error {
	return godotenv.Load(envFile)
}

// expandEnvVars 对配置内容执行 ${VAR} 环境变量替换
func expandEnvVars(data []byte) []byte {
	return []byte(os.ExpandEnv(string(data)))
}
