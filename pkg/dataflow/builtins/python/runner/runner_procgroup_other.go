//go:build !linux && !darwin

package runner

// setupProcessGroup 非 Unix 平台的空实现，始终返回 false。
// Windows 平台不支持进程组管理，Close() 将回退到单进程终止。
func setupProcessGroup(_ int) bool {
	return false
}

// killProcessGroup 非 Unix 平台的空实现。
func killProcessGroup(_ int) error {
	return nil
}
