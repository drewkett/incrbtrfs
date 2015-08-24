package main

import (
	"os"
	"os/exec"
)

type CmdRunner struct {
	Started chan error
	Done    chan error
	Signal  chan os.Signal
}

func NewCmdRunner() (runner CmdRunner) {
	runner.Started = make(chan error)
	runner.Done = make(chan error)
	runner.Signal = make(chan os.Signal)
	return
}

func RunCommand(cmd *exec.Cmd) CmdRunner {
	runner := NewCmdRunner()
	go func() {
		err := cmd.Start()
		runner.Started <- err
		if err != nil {
			runner.Done <- err
			return
		}
		runner.Done <- cmd.Wait()
	}()
	go func() {
		for {
			select {
			case sig := <-runner.Signal:
				cmd.Process.Signal(sig)
			}
		}
	}()
	return runner
}
