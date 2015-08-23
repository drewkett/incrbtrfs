package main

import (
	"os"
	"os/exec"
)

// DeleteSnapshot tries to delete a btrfs snaphot. Returns an error if it fail
func DeleteSnapshot(location string) (err error) {
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "delete", location)
	if *verboseFlag {
		printCommand(btrfsCmd)
		btrfsCmd.Stdout = os.Stderr
		btrfsCmd.Stderr = os.Stderr
	}
	err = btrfsCmd.Run()
	return
}

type CmdWatcher struct {
	Started chan error
	Done    chan error
}

func NewCmdWatcher() (cw CmdWatcher) {
	cw.Started = make(chan error)
	cw.Done = make(chan error)
	return
}
