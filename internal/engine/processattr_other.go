//go:build !windows

package engine

import "os/exec"

func hideCommandWindow(cmd *exec.Cmd) {}
