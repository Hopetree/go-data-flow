// Package main 是 dataflow 服务的入口点
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/app"
	"github.com/Hopetree/go-data-flow/pkg/logger"
)

// 构建时注入的版本信息
var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// 命令行参数
var (
	appConfFile  string
	configFile   string
	configDir    string
	configs      string
	envFile      string
	listOnly     bool
	showVersion  bool
	validateOnly bool
	envCheckOnly bool
	listFlows    bool
)

func init() {
	flag.StringVar(&appConfFile, "a", "", "应用配置文件路径 (config/app.yaml)")
	flag.StringVar(&configFile, "c", "", "Flow 配置文件路径")
	flag.StringVar(&configDir, "d", "", "配置文件目录")
	flag.StringVar(&configs, "C", "", "多个配置文件，逗号分隔")
	flag.StringVar(&envFile, "e", "", ".env 文件路径 (默认: .env)")
	flag.BoolVar(&listOnly, "l", false, "列出所有组件")
	flag.BoolVar(&showVersion, "v", false, "显示版本")
	flag.BoolVar(&validateOnly, "validate", false, "只验证配置，不执行 Flow")
	flag.BoolVar(&envCheckOnly, "env-check", false, "检查环境变量是否已设置")
	flag.BoolVar(&listFlows, "list-flows", false, "列出发现的 Flow 配置文件")
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
		EnvFile:     envFile,
	})

	// 注册内置组件
	app.RegisterAllBuiltins(application.Registry())

	// 列出组件
	if listOnly {
		application.ListComponents()
		return
	}

	// 收集配置文件
	configFiles := application.CollectConfigFiles()

	// --list-flows：列出发现的 Flow 配置文件
	if listFlows {
		if len(configFiles) == 0 {
			fmt.Println("没有找到配置文件")
			os.Exit(1)
		}
		for _, f := range configFiles {
			fmt.Println(f)
		}
		return
	}

	// --env-check：检查环境变量是否已设置
	if envCheckOnly {
		if len(configFiles) == 0 {
			fmt.Println("没有找到配置文件")
			os.Exit(1)
		}
		missing := app.CheckEnvVars(configFiles)
		if len(missing) > 0 {
			fmt.Printf("缺少以下环境变量:\n")
			for _, v := range missing {
				fmt.Printf("  %s\n", v)
			}
			os.Exit(1)
		}
		fmt.Println("所有环境变量检查通过")
		return
	}

	// --validate：只验证配置，不执行 Flow
	if validateOnly {
		if len(configFiles) == 0 {
			fmt.Println("没有找到配置文件")
			os.Exit(1)
		}
		if err := app.ValidateConfigs(configFiles); err != nil {
			fmt.Printf("配置验证失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("所有配置验证通过")
		return
	}

	// 检查是否有配置
	if len(configFiles) == 0 {
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
