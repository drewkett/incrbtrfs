package main

import (
	"log"
	"os"
	"os/exec"
	"strings"
)

type Subvolume struct {
	Directory    string
	SnapshotsLoc SnapshotsLoc
	Remotes      []RemoteSnapshotsLoc
}

func (subvolume Subvolume) Print() {
	log.Printf("Subvolume='%s'", subvolume.Directory)
	log.Printf("Snapshot Dir='%s' (%s)\n", subvolume.SnapshotsLoc.Directory, subvolume.SnapshotsLoc.Limits.String())
	for _, remote := range subvolume.Remotes {
		dst := remote.SnapshotsLoc.Directory
		if remote.Host != "" {
			dst = strings.Join([]string{remote.Host, dst}, ":")
			if remote.User != "" {
				dst = strings.Join([]string{remote.User, dst}, "@")
			}
		}
		log.Printf("Remote Dir='%s' (%s)\n", dst, remote.SnapshotsLoc.Limits.String())
	}

}

func (subvolume Subvolume) RunSnapshot(timestamp Timestamp) (err error) {
	snapshot := Snapshot{subvolume.SnapshotsLoc, timestamp}
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "snapshot", "-r", subvolume.Directory, snapshot.Path())
	if *verboseFlag {
		printCommand(btrfsCmd)
		btrfsCmd.Stdout = os.Stderr
		btrfsCmd.Stderr = os.Stderr
	}
	err = btrfsCmd.Run()
	if err != nil {
		if _, errTmp := os.Stat(snapshot.Path()); !os.IsNotExist(errTmp) {
			errTmp = snapshot.DeleteSnapshot()
			if errTmp != nil {
				if !(*quietFlag) {
					log.Println("Failed to deleted to failed snapshot")
				}
			}
		}
		return
	}

	timestamps, err := subvolume.SnapshotsLoc.ReadTimestampsDir()
	if err != nil {
		return
	}
	for _, remote := range subvolume.Remotes {
		var remoteTimestamps []Timestamp
		if remote.Host == "" {
			remoteTimestamps, err = remote.SnapshotsLoc.ReadTimestampsDir()
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
		err = remote.SendSnapshot(snapshot, parentTimestamp)
		if err != nil {
			log.Println("Error sending snapshot")
			log.Println(err.Error())
			err = nil
			continue
		}
	}
	_, err = subvolume.SnapshotsLoc.CleanUp(timestamp, timestamps)
	if err != nil {
		return
	}

	return
}
