// Package main 是 dataflow 服务的入口点
package main

import (
	"context"
	"flag"
	"os"

	"github.com/yourorg/go-data-flow/pkg/dataflow/app"
	"github.com/yourorg/go-data-flow/pkg/logger"
)

// 构建时注入的版本信息
var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// 命令行参数
var (
	appConfFile string
	configFile  string
	configDir   string
	configs     string
	listOnly    bool
	showVersion bool
)

func init() {
	flag.StringVar(&appConfFile, "a", "", "应用配置文件路径 (config/app.yaml)")
	flag.StringVar(&configFile, "c", "", "Flow 配置文件路径")
	flag.StringVar(&configDir, "d", "", "配置文件目录")
	flag.StringVar(&configs, "C", "", "多个配置文件，逗号分隔")
	flag.BoolVar(&listOnly, "l", false, "列出所有组件")
	flag.BoolVar(&showVersion, "v", false, "显示版本")
}

func main() {
	flag.Parse()

	// 设置版本信息
	app.Version = Version
	app.BuildTime = BuildTime
	app.GitCommit = GitCommit

	// 显示版本
	if showVersion {
		app.PrintVersion()
		return
	}

	// 创建应用
	application := app.NewApp(app.Options{
		AppConfFile: appConfFile,
		ConfigFile:  configFile,
		ConfigDir:   configDir,
		Configs:     configs,
	})

	// 注册内置组件
	app.RegisterAllBuiltins(application.Registry())

	// 列出组件
	if listOnly {
		application.ListComponents()
		return
	}

	// 检查是否有配置
	if configFile == "" && configDir == "" && configs == "" {
		app.PrintUsage()
		os.Exit(1)
	}

	// 创建上下文
	ctx := context.Background()

	// 运行应用
	if err := application.Run(ctx); err != nil {
		logger.Error("运行失败: %v", err)
		os.Exit(1)
	}
}
