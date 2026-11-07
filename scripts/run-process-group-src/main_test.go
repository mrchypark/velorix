package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSignalStopsOwnedProcessGroupDescendants(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("POSIX shell is unavailable")
	}
	pidPath := filepath.Join(t.TempDir(), "child.pid")
	command := exec.Command("sh", "-c", "sleep 60 & echo $! > \"$1\"; wait", "sh", pidPath)
	signals := make(chan os.Signal, 1)
	done := make(chan int, 1)
	go func() { done <- runCommand(command.Args, signals, 300*time.Millisecond) }()

	var childPID int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(pidPath)
		if err == nil {
			childPID, err = strconv.Atoi(strings.TrimSpace(string(contents)))
			if err == nil {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID == 0 {
		t.Fatal("stub descendant did not start")
	}

	signals <- syscall.SIGTERM
	select {
	case code := <-done:
		if code != 128+int(syscall.SIGTERM) {
			t.Fatalf("runCommand exit code = %d, want SIGTERM status", code)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("runCommand did not exit after SIGTERM")
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(childPID, 0); err == syscall.ESRCH {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("owned descendant process %d remains after group termination", childPID)
}
