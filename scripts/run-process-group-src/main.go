package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

const terminationGrace = 3 * time.Second
const descendantCleanupGrace = 100 * time.Millisecond

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: run-process-group COMMAND [ARG ...]")
		os.Exit(64)
	}
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	os.Exit(runCommand(os.Args[1:], signals, terminationGrace))
}

func runCommand(argv []string, signals <-chan os.Signal, grace time.Duration) int {
	command := exec.Command(argv[0], argv[1:]...)
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start %s: %v\n", argv[0], err)
		return 127
	}

	processGroup := command.Process.Pid
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()

	select {
	case err := <-waited:
		terminateDescendants(processGroup, grace)
		return exitCode(err)
	case received := <-signals:
		signalGroup(processGroup, received)
		select {
		case <-waited:
			killGroup(processGroup)
		case <-time.After(grace):
			killGroup(processGroup)
			<-waited
		}
		return 128 + signalNumber(received)
	}
}

func terminateDescendants(processGroup int, grace time.Duration) {
	if syscall.Kill(-processGroup, 0) != nil {
		return
	}
	signalGroup(processGroup, syscall.SIGTERM)
	if grace <= 0 {
		grace = descendantCleanupGrace
	}
	time.Sleep(min(grace, descendantCleanupGrace))
	killGroup(processGroup)
}

func signalGroup(processGroup int, received os.Signal) {
	if signalValue, ok := received.(syscall.Signal); ok {
		_ = syscall.Kill(-processGroup, signalValue)
	}
}

func killGroup(processGroup int) {
	_ = syscall.Kill(-processGroup, syscall.SIGKILL)
}

func signalNumber(received os.Signal) int {
	if signalValue, ok := received.(syscall.Signal); ok {
		return int(signalValue)
	}
	return int(syscall.SIGTERM)
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return exitError.ExitCode()
	}
	return 1
}
