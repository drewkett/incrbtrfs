package main

import (
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
)

type Subvolume struct {
	Directory    string
	SnapshotsLoc SnapshotsLoc
	Remotes      []RemoteSnapshotsLoc
}

func (subvolume Subvolume) Print() {
	if verbosity > 0 {
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
		if verbosity > 0 {
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
	defer lock.Unlock()
	timestamp := getCurrentTimestamp()
	snapshot := Snapshot{subvolume.SnapshotsLoc, timestamp}
	err = os.MkdirAll(path.Dir(snapshot.Path()), dirMode)
	if err != nil {
		return
	}
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "snapshot", "-r", subvolume.Directory, snapshot.Path())
	if verbosity > 1 {
		printCommand(btrfsCmd)
		btrfsCmd.Stdout = os.Stderr
		btrfsCmd.Stderr = os.Stderr
	}
	err = btrfsCmd.Run()
	if err != nil {
		if verbosity > 0 {
			log.Println("Snapshot failed")
		}
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
	if *pinnedFlag {
		pinDir := path.Join(subvolume.SnapshotsLoc.Directory, "pinned")
		err = os.MkdirAll(pinDir, dirMode)
		if err != nil {
			return
		}
		src := path.Join("..", "timestamp", string(timestamp))
		dst := path.Join(pinDir, string(timestamp))
		if verbosity > 1 {
			log.Printf("Symlink '%s' => '%s'\n", dst, src)
		}
		err = os.Symlink(src, dst)
		if err != nil {
			return
		}
	}

	timestamps, err := subvolume.SnapshotsLoc.ReadTimestampsDir()
	if err != nil {
		return
	}
	for _, remote := range subvolume.Remotes {
		err = remote.sendSnapshotUsingParent(snapshot, timestamps)
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
