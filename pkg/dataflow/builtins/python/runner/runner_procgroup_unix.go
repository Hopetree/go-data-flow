//go:build linux || darwin

package runner

import (
	"golang.org/x/sys/unix"
)

// setupProcessGroup 将进程设为独立进程组。
// 返回是否设置成功，用于 Close() 中选择进程终止策略。
// 注意：此函数在 cmd.Start() 之后调用，存在微小竞态窗口（纳秒级），
// 但 Python 解释器初始化需要时间，实际上子进程来不及 fork。
func setupProcessGroup(pid int) bool {
	if err := unix.Setpgid(pid, pid); err != nil {
		return false
	}
	return true
}

// killProcessGroup 杀死整个进程组（pid 为负值表示进程组）。
func killProcessGroup(pid int) error {
	return unix.Kill(-pid, unix.SIGKILL)
}
