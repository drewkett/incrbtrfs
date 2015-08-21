package main

import "os/exec"

// DeleteSnapshot tries to delete a btrfs snaphot. Returns an error if it fail
func DeleteSnapshot(location string) (err error) {
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "delete", location)
	if *verboseFlag {
		printCommand(btrfsCmd)
	}
	err = btrfsCmd.Run()
	return
}
