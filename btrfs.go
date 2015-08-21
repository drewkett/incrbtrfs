package main

import (
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

func ReceiveSnapshot(location string) (err error) {
	targetPath := path.Join(location, "timestamp")
	receiveCmd := exec.Command(btrfsBin, "receive", targetPath)
	receiveCmd.Stdin = os.Stdin
	receiveOut, err := receiveCmd.CombinedOutput()
	if err != nil {
		if !(*quietFlag) {
			log.Printf(string(receiveOut))
		}
		if _, errTmp := os.Stat(targetPath); !os.IsNotExist(errTmp) {
			errTmp = DeleteSnapshot(targetPath)
			if errTmp != nil {
				if *verboseFlag {
					log.Println("Failed to deleted to failed snapshot")
				}
			}
		}
		return
	}
	return
}
