package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
)

// DeleteSnapshot tries to delete a btrfs snaphot. Returns an error if it fail
func DeleteSnapshot(location string) (err error) {
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "delete", location)
	if *verboseFlag {
		printCommand(btrfsCmd)
	}
	err = btrfsCmd.Run()
	return
}

type CmdWatcher struct {
	Started chan error
	Done    chan error
}

func NewCmdWatcher() (cw CmdWatcher) {
	cw.Started = make(chan error, 1)
	cw.Done = make(chan error)
	return
}

func ReceiveSnapshot(in io.Reader, location string, watcher CmdWatcher) {
	if *debugFlag {
		log.Println("ReceiveSnapshot")
	}
	var out bytes.Buffer
	targetPath := path.Join(location, "timestamp")
	err := os.MkdirAll(targetPath, 0700|os.ModeDir)
	log.Println("ReceiveSnapshot: MkdirAll")
	if err != nil {
		watcher.Started <- err
		watcher.Done <- err
		return
	}
	receiveCmd := exec.Command(btrfsBin, "receive", targetPath)
	if *verboseFlag {
		printCommand(receiveCmd)
	}
	receiveCmd.Stdin = in
	receiveCmd.Stdout = &out
	err = receiveCmd.Start()
	if *debugFlag {
		log.Println("ReceiveSnapshot: Cmd Started")
	}
	if err != nil {
		if *verboseFlag {
			log.Print(out.String())
		}
		watcher.Started <- err
		watcher.Done <- err
		return
	}
	watcher.Started <- nil
	if *debugFlag {
		log.Println("ReceiveSnapshot: Cmd Sent Started")
	}
	err = receiveCmd.Wait()
	if *debugFlag {
		log.Println("ReceiveSnapshot: Cmd Wait")
	}
	if err != nil {
		if _, errTmp := os.Stat(targetPath); !os.IsNotExist(errTmp) {
			errTmp = DeleteSnapshot(targetPath)
			if errTmp != nil {
				out.WriteString("Couldn't to delete to failed snapshot\n")
			}
		}
	}
	if *verboseFlag {
		log.Print(out.String())
	}
	watcher.Done <- err
	close(watcher.Started)
	return
}
