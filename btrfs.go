package main

type CmdWatcher struct {
	Started chan error
	Done    chan error
}

func NewCmdWatcher() (cw CmdWatcher) {
	cw.Started = make(chan error)
	cw.Done = make(chan error)
	return
}
