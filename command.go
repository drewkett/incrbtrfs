package main

import (
	"os"
	"os/exec"
)

type CmdWatcher struct {
	Started chan error
	Done    chan error
	Signal  chan os.Signal
}

func NewCmdWatcher() (cw CmdWatcher) {
	cw.Started = make(chan error)
	cw.Done = make(chan error)
	return
}

func RunCommand(cmd *exec.Cmd) CmdWatcher {
	var cw CmdWatcher
	cw.Started = make(chan error)
	cw.Done = make(chan error)
	cw.Signal = make(chan os.Signal)
	go func(cw *CmdWatcher, cmd *exec.Cmd) {
		err := cmd.Start()
		if err != nil {
			cw.Started <- err
			cw.Done <- err
		}
		cw.Started <- nil
		cw.Done <- cmd.Wait()
	}(&cw, cmd)
	go func(cw *CmdWatcher, cmd *exec.Cmd) {
		for {
			select {
			case sig := <-cw.Signal:
				cmd.Process.Signal(sig)
			}
		}
	}(&cw, cmd)
	return cw
}
