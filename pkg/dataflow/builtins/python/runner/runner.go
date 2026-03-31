// Package runner 提供 Python 子进程运行器。
// 管理 Python 进程的启动、stdin/stdout JSON lines 通信和生命周期。
package runner

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Hopetree/go-data-flow/pkg/logger"
)

// ConvertEnv 将 map[string]interface{} 转为 map[string]string。
// 用于兼容 YAML 中非字符串类型的环境变量值（如 WORKER_COUNT: 8）。
func ConvertEnv(raw map[string]interface{}) map[string]string {
	result := make(map[string]string, len(raw))
	for k, v := range raw {
		result[k] = fmt.Sprintf("%v", v)
	}
	return result
}

// 最大行缓冲区大小（10MB），防止大记录被截断
const maxLineBufferSize = 10 * 1024 * 1024

// 进程退出等待超时
const processExitTimeout = 30 * time.Second

// readResult 是 stdout 读取结果
type readResult struct {
	data []byte
	err  error
}

// Runner 管理 Python 子进程的生命周期和通信。
type Runner struct {
	pythonExec string // Python 可执行文件路径，默认 "python3"
	scriptPath string // 脚本绝对路径
	args       []string
	env        []string // 额外环境变量

	cmd       *exec.Cmd
	stdinPipe io.WriteCloser
	writer    *bufio.Writer // 复用的 stdin writer

	closeOnce sync.Once
	started   bool

	// stdout 读取通道：由专用协程写入，ReadLine 从中读取
	lineCh chan readResult

	// stderr 日志回调
	onStderr func(line string)
}

// Config Runner 的配置。
type Config struct {
	// Script Python 脚本路径（必填）
	Script string `json:"script"`
	// PythonExec Python 可执行文件路径，默认 "python3"
	PythonExec string `json:"python_exec"`
	// Args 传递给脚本的额外命令行参数
	Args []string `json:"args"`
	// Env 额外的环境变量（会合并到进程环境中）
	Env map[string]string `json:"env"`
}

// New 创建 Runner 实例。
// 解析配置，转绝对路径，验证脚本存在。
func New(cfg Config) (*Runner, error) {
	if cfg.Script == "" {
		return nil, fmt.Errorf("未指定 Python 脚本路径")
	}

	// 转绝对路径
	absPath, err := filepath.Abs(cfg.Script)
	if err != nil {
		return nil, fmt.Errorf("解析脚本路径失败: %w", err)
	}

	// 验证脚本存在
	if _, err := os.Stat(absPath); err != nil {
		return nil, fmt.Errorf("Python 脚本不存在: %s: %w", absPath, err)
	}

	pythonExec := cfg.PythonExec
	if pythonExec == "" {
		pythonExec = "python3"
	}

	// 构建环境变量列表
	var envList []string
	for k, v := range cfg.Env {
		envList = append(envList, fmt.Sprintf("%s=%s", k, v))
	}

	return &Runner{
		pythonExec: pythonExec,
		scriptPath: absPath,
		args:       cfg.Args,
		env:        envList,
	}, nil
}

// SetStderrCallback 设置 stderr 日志回调函数。
// 每当 Python 进程输出一行 stderr 时调用。
func (r *Runner) SetStderrCallback(fn func(line string)) {
	r.onStderr = fn
}

// DefaultStderrHandler 返回一个默认的 stderr 日志处理器。
// 自动解析 Python logging 的级别前缀（如 INFO:、WARNING:、ERROR:），
// 并使用对应的 Go 日志级别输出。未识别的前缀默认为 WARN。
// prefix 为日志来源标识，如 "[python-processor]"。
func DefaultStderrHandler(prefix string) func(string) {
	return func(line string) {
		// 解析 Python logging 格式：INFO:message 或 WARNING:message
		if idx := strings.Index(line, ":"); idx > 0 {
			level := strings.TrimSpace(line[:idx])
			msg := strings.TrimSpace(line[idx+1:])
			switch level {
			case "DEBUG":
				logger.Debug("[%s] %s", prefix, msg)
				return
			case "INFO":
				logger.Info("[%s] %s", prefix, msg)
				return
			case "WARNING":
				logger.Warn("[%s] %s", prefix, msg)
				return
			case "ERROR", "CRITICAL":
				logger.Error("[%s] %s", prefix, msg)
				return
			}
		}
		// 未识别的格式，默认 WARN
		logger.Warn("[%s] %s", prefix, line)
	}
}

// Start 启动 Python 子进程，建立 stdin/stdout/stderr 管道。
// 启动后 stdout 由专用协程持续读取并通过 ReadLine 获取。
func (r *Runner) Start(ctx context.Context) error {
	// 构建命令参数
	args := []string{r.scriptPath}
	args = append(args, r.args...)

	cmd := exec.CommandContext(ctx, r.pythonExec, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// 合并环境变量
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, r.env...)

	// 连接管道
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("创建 stdin 管道失败: %w", err)
	}
	r.stdinPipe = stdinPipe
	r.writer = bufio.NewWriter(stdinPipe)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		stdinPipe.Close()
		return fmt.Errorf("创建 stdout 管道失败: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		stdinPipe.Close()
		stdoutPipe.Close()
		return fmt.Errorf("创建 stderr 管道失败: %w", err)
	}

	// 启动进程
	if err := cmd.Start(); err != nil {
		stdinPipe.Close()
		stdoutPipe.Close()
		stderrPipe.Close()
		return fmt.Errorf("启动 Python 进程失败: %w", err)
	}

	r.cmd = cmd
	r.started = true

	// 启动 stdout 读取协程（单一协程负责读取，避免 bufio.Scanner 并发问题）
	r.lineCh = make(chan readResult, 100)
	go r.readStdout(stdoutPipe)

	// 启动 stderr 读取协程（防止 stderr 缓冲区满导致死锁）
	go r.drainStderr(stderrPipe)

	return nil
}

// readStdout 持续读取 stdout 并发送到 lineCh。
// 当 stdout 关闭（Python 退出）时自动结束。
func (r *Runner) readStdout(pipe io.ReadCloser) {
	defer close(r.lineCh)
	defer pipe.Close()

	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineBufferSize)

	for scanner.Scan() {
		r.lineCh <- readResult{data: []byte(scanner.Text()), err: nil}
	}

	// 发送 EOF
	if err := scanner.Err(); err != nil {
		r.lineCh <- readResult{data: nil, err: err}
	}
}

// drainStderr 持续读取 stderr 并通过回调输出。
func (r *Runner) drainStderr(pipe io.ReadCloser) {
	defer pipe.Close()

	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineBufferSize)

	for scanner.Scan() {
		if r.onStderr != nil {
			r.onStderr(scanner.Text())
		}
	}
}

// WriteLine 向 Python stdin 写入一行数据。
func (r *Runner) WriteLine(ctx context.Context, line []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if !r.started {
		return fmt.Errorf("进程未启动")
	}

	if _, err := r.writer.Write(line); err != nil {
		return fmt.Errorf("写入 stdin 失败: %w", err)
	}
	if _, err := r.writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("写入 stdin 换行失败: %w", err)
	}
	return r.writer.Flush()
}

// ReadLine 从 Python stdout 读取一行数据。
// 返回 io.EOF 表示 Python 进程已退出（stdout 关闭）。
func (r *Runner) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res, ok := <-r.lineCh:
		if !ok {
			// channel 关闭，Python 进程已退出
			return nil, io.EOF
		}
		return res.data, res.err
	}
}

// CloseStdin 关闭 Python 进程的 stdin，通知 Python 输入结束。
// 不等待进程退出，由 Python 自行处理剩余数据后退出。
func (r *Runner) CloseStdin() error {
	if r.stdinPipe != nil {
		return r.stdinPipe.Close()
	}
	return nil
}

// Close 关闭 stdin 并等待进程退出。
// 如果进程超时未退出则发送 SIGKILL。
// 幂等操作，多次调用安全。
func (r *Runner) Close() error {
	var closeErr error
	r.closeOnce.Do(func() {
		if !r.started {
			return
		}

		// 关闭 stdin
		if r.stdinPipe != nil {
			r.stdinPipe.Close()
		}

		// 等待进程退出，带超时
		if r.cmd != nil && r.cmd.Process != nil {
			done := make(chan error, 1)
			go func() {
				done <- r.cmd.Wait()
			}()

			select {
			case err := <-done:
				if err != nil {
					closeErr = fmt.Errorf("Python 进程退出异常: %w", err)
				}
			case <-time.After(processExitTimeout):
				// 超时，杀死整个进程组
				syscall.Kill(-r.cmd.Process.Pid, syscall.SIGKILL)
				closeErr = fmt.Errorf("Python 进程超时未退出，已强制杀死")
			}
		}
	})
	return closeErr
}
