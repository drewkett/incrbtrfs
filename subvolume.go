package main

import (
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
)

type Subvolume struct {
	Directory   string
	SnapshotLoc SnapshotLoc
	Remotes     []RemoteSnapshotLoc
}

func (subvolume Subvolume) Print() {
	log.Printf("Subvolume='%s'", subvolume.Directory)
	log.Printf("Snapshot Dir='%s' (%s)\n", subvolume.SnapshotLoc.Directory, subvolume.SnapshotLoc.Limits.String())
	for _, remote := range subvolume.Remotes {
		dst := remote.SnapshotLoc.Directory
		if remote.Host != "" {
			dst = strings.Join([]string{remote.Host, dst}, ":")
			if remote.User != "" {
				dst = strings.Join([]string{remote.User, dst}, "@")
			}
		}
		log.Printf("Remote Dir='%s' (%s)\n", dst, remote.SnapshotLoc.Limits.String())
	}

}

func (subvolume Subvolume) RunSnapshot(timestamp Timestamp) (err error) {
	timestampPath := path.Join(subvolume.SnapshotLoc.Directory, "timestamp")
	targetPath := path.Join(timestampPath, string(timestamp))
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "snapshot", "-r", subvolume.Directory, targetPath)
	if *verboseFlag {
		printCommand(btrfsCmd)
	}
	output, err := btrfsCmd.CombinedOutput()
	if err != nil {
		if !(*quietFlag) {
			log.Printf("%s", output)
		}
		if _, errTmp := os.Stat(targetPath); !os.IsNotExist(errTmp) {
			errTmp = DeleteSnapshot(targetPath)
			if errTmp != nil {
				if !(*quietFlag) {
					log.Println("Failed to deleted to failed snapshot")
				}
			}
		}
		return
	}
	if *verboseFlag {
		log.Printf("%s", output)
	}

	timestamps, err := subvolume.SnapshotLoc.ReadTimestampsDir()
	if err != nil {
		return
	}
	err = subvolume.SnapshotLoc.CleanUp(timestamp, timestamps)
	if err != nil {
		return
	}

	for _, remote := range subvolume.Remotes {
		var remoteTimestamps []Timestamp
		if remote.Host == "" {
			remoteTimestamps, err = remote.SnapshotLoc.ReadTimestampsDir()
			if err != nil {
				log.Println(err.Error())
				err = nil
				continue
			}
		} else {
			remoteTimestamps, err = remote.GetTimestamps()
			if err != nil {
				log.Println(err.Error())
				err = nil
				continue
			}
		}
		parentTimestamp := calcParent(timestamps, remoteTimestamps)
		if *verboseFlag && parentTimestamp != "" {
			log.Printf("Parent = %s\n", string(parentTimestamp))
		}
		err = remote.SendSnapshot(timestampPath, timestamp, parentTimestamp)
		if err != nil {
			log.Println("Error sending snapshot")
			log.Println(err.Error())
			err = nil
			continue
		}
	}
	return
}
