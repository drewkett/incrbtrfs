package main

import (
	"os"
	"os/exec"
	"path"
)

type Snapshot struct {
	snapshotsLoc SnapshotsLoc
	timestamp    Timestamp
}

func (s Snapshot) Path() string {
	return path.Join(s.snapshotsLoc.Directory, "timestamp", string(s.timestamp))
}

// DeleteSnapshot tries to delete a btrfs snaphot. Returns an error if it fail
func (s Snapshot) DeleteSnapshot() (err error) {
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "delete", s.Path())
	if *verboseFlag {
		printCommand(btrfsCmd)
		btrfsCmd.Stdout = os.Stderr
		btrfsCmd.Stderr = os.Stderr
	}
	err = btrfsCmd.Run()
	return
}

func (s Snapshot) TagSuccess() (err error) {
	successPath := path.Join(s.snapshotsLoc.Directory, "success")
	err = os.MkdirAll(successPath, dirMode)
	if err != nil {
		return
	}
	dstPath := path.Join(successPath, string(s.timestamp))
	srcPath := path.Join("..", "timestamp", string(s.timestamp))
	err = os.Symlink(srcPath, dstPath)
	return
}
