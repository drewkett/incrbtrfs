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
	if verbosity > 1 {
		log.Printf("Subvolume='%s'", subvolume.Directory)
		log.Printf("Snapshot Dir='%s' (%s)\n", subvolume.SnapshotsLoc.Directory, subvolume.SnapshotsLoc.Limits.String())
	}
	for _, remote := range subvolume.Remotes {
		dst := remote.SnapshotsLoc.Directory
		if remote.Host != "" {
			dst = strings.Join([]string{remote.Host, dst}, ":")
			if remote.User != "" {
				dst = strings.Join([]string{remote.User, dst}, "@")
			}
		}
		if verbosity > 1 {
			log.Printf("Remote Dir='%s' (%s)\n", dst, remote.SnapshotsLoc.Limits.String())
		}
	}

}

func (subvolume Subvolume) RunSnapshot() (err error) {
	//TODO Move Lock to after snapshot. To allow snapshots if a previous long send
	//is still running. Need to implement some guarantee that two instances don't
	//try to create the same snapshot at the same time and then delete cause one
	//will fail
	lock, err := NewDirLock(subvolume.SnapshotsLoc.Directory)
	if err != nil {
		return
	}
	timestamp := getCurrentTimestamp()
	snapshot := Snapshot{subvolume.SnapshotsLoc, timestamp}
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "snapshot", "-r", subvolume.Directory, snapshot.Path())
	if verbosity > 1 {
		printCommand(btrfsCmd)
		btrfsCmd.Stdout = os.Stderr
		btrfsCmd.Stderr = os.Stderr
	}
	err = btrfsCmd.Run()
	if err != nil {
		if _, errTmp := os.Stat(snapshot.Path()); !os.IsNotExist(errTmp) {
			errTmp = snapshot.DeleteSnapshot()
			if errTmp != nil {
				if verbosity > 0 {
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
		var lock DirLock
		if remote.Host == "" {
			lock, err = NewDirLock(remote.SnapshotsLoc.Directory)
			if err != nil {
				log.Println(err.Error())
				err = nil
				continue
			}
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
		if verbosity > 0 && parentTimestamp != "" {
			log.Printf("Parent = %s\n", string(parentTimestamp))
		}
		err = remote.SendSnapshot(snapshot, parentTimestamp)
		if err != nil {
			log.Println("Error sending snapshot")
			log.Println(err.Error())
			err = nil
			continue
		}
		if remote.Host == "" {
			err = lock.Unlock()
			if err != nil {
				log.Println(err.Error())
				err = nil
			}
		}
	}
	_, err = subvolume.SnapshotsLoc.CleanUp(timestamp, timestamps)
	if err != nil {
		return
	}
	err = lock.Unlock()
	if err != nil {
		return
	}
	return
}
