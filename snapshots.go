package main

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"
)

type SnapshotsLoc struct {
	Directory string
	Limits    Limits
}

func removeAllSymlinks(dir string) (err error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}
	for _, fi := range fileInfos {
		if fi.Mode()&os.ModeSymlink != 0 {
			err = os.Remove(path.Join(dir, fi.Name()))
			if err != nil {
				return
			}
		}
	}
	return
}

func (snapshotsLoc SnapshotsLoc) clean(interval Interval, now time.Time, timestamps []Timestamp) (keptTimestampsMap TimestampMap, err error) {
	dir := path.Join(snapshotsLoc.Directory, string(interval))
	err = os.MkdirAll(dir, os.ModeDir|0700)
	if err != nil {
		return
	}
	err = removeAllSymlinks(dir)
	if err != nil {
		return
	}
	maxIndex := interval.GetMaxIndex(snapshotsLoc.Limits)
	keptIndices := make(map[int]bool)
	keptTimestampsMap = make(TimestampMap)
	for _, timestamp := range timestamps {
		var i int
		var snapshotTime time.Time
		snapshotTime, err = parseTimestamp(timestamp)
		if err != nil {
			continue
		}
		i = interval.CalcIndex(now, snapshotTime)
		if i >= maxIndex {
			continue
		}
		if _, ok := keptIndices[i]; ok {
			continue
		}
		keptIndices[i] = true
		keptTimestampsMap[timestamp] = true
		src := path.Join("..", "timestamp", string(timestamp))
		dst := path.Join(dir, strconv.Itoa(i))
		if *verboseFlag {
			log.Printf("Symlink '%s' => '%s'\n", dst, src)
		}
		err = os.Symlink(src, dst)
		if err != nil {
			return
		}
	}
	return
}

func (snapshotsLoc SnapshotsLoc) CleanUp(nowTimestamp Timestamp, timestamps []Timestamp) (keptTimestamps []Timestamp, err error) {
	if *debugFlag {
		log.Println("Running Clean Up")
	}
	now, err := parseTimestamp(nowTimestamp)
	if err != nil {
		return
	}
	keptTimestampsMap := make(TimestampMap)
	keptTimestampsMap[nowTimestamp] = true
	var tempMap TimestampMap
	for _, interval := range Intervals {
		tempMap, err = snapshotsLoc.clean(interval, now, timestamps)
		if err != nil {
			return
		}
		keptTimestampsMap = keptTimestampsMap.Merge(tempMap)
	}
	// Remove unneeded timestamps
	for _, timestamp := range timestamps {
		if _, ok := keptTimestampsMap[timestamp]; ok {
			keptTimestamps = append(keptTimestamps, timestamp)
		} else {
			snapshot := Snapshot{snapshotsLoc, timestamp}
			err = snapshot.DeleteSnapshot()
			if err != nil {
				return
			}
		}
	}
	return
}

func (snapshotsLoc SnapshotsLoc) ReceiveSnapshot(in io.Reader, timestamp Timestamp, watcher CmdWatcher) {
	if *debugFlag {
		log.Println("ReceiveSnapshot")
	}
	targetPath := path.Join(snapshotsLoc.Directory, "timestamp")
	err := os.MkdirAll(targetPath, 0700|os.ModeDir)
	if *debugFlag {
		log.Println("ReceiveSnapshot: MkdirAll")
	}
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
	if *verboseFlag {
		receiveCmd.Stdout = os.Stderr
		receiveCmd.Stderr = os.Stderr
	}
	err = receiveCmd.Start()
	if *debugFlag {
		log.Println("ReceiveSnapshot: Cmd Started")
	}
	if err != nil {
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
		snapshot := Snapshot{snapshotsLoc, timestamp}
		if _, errTmp := os.Stat(snapshot.Path()); !os.IsNotExist(errTmp) {
			errTmp = snapshot.DeleteSnapshot()
		}
	}
	watcher.Done <- err
	return
}

func (snapshotsLoc SnapshotsLoc) ReceiveAndCleanUp(in io.Reader, timestamp Timestamp, cw CmdWatcher) {
	if *debugFlag {
		log.Println("ReceiveAndCleanup")
	}
	subCW := NewCmdWatcher()
	go snapshotsLoc.ReceiveSnapshot(in, timestamp, subCW)
	if *debugFlag {
		log.Println("ReceiveAndCleanup: go ReceiveSnapshot")
	}
	err := <-subCW.Started
	if *debugFlag {
		log.Println("ReceiveAndCleanup: Receive Started")
	}
	if err != nil {
		cw.Started <- err
		cw.Done <- err
		return
	}
	if *debugFlag {
		log.Println("ReceiveAndCleanup: Started")
	}
	cw.Started <- nil
	if *debugFlag {
		log.Println("ReceiveAndCleanup: Sent Started")
	}
	err = <-subCW.Done
	if *debugFlag {
		log.Println("ReceiveAndCleanup: Receive Done")
	}
	if err != nil {
		cw.Done <- err
		return
	}
	if *debugFlag {
		log.Println("ReceiveAndCleanup: ReadTimestampsDir")
	}
	timestamps, err := snapshotsLoc.ReadTimestampsDir()
	if err != nil {
		cw.Done <- err
		return
	}
	if *debugFlag {
		log.Println("ReceiveAndCleanup: CleanUp")
	}
	_, err = snapshotsLoc.CleanUp(timestamp, timestamps)
	cw.Done <- err
	return
}

func (snapshotsLoc SnapshotsLoc) ReadTimestampsDir() (timestamps []Timestamp, err error) {
	timestampsDir := path.Join(snapshotsLoc.Directory, "timestamp")
	os.MkdirAll(timestampsDir, 0700|os.ModeDir)
	fileInfos, err := ioutil.ReadDir(timestampsDir)
	if err != nil {
		return
	}
	for _, fi := range fileInfos {
		if fi.IsDir() {
			timestamp := Timestamp(fi.Name())
			_, err := parseTimestamp(timestamp)
			if err != nil {
				continue
			}
			timestamps = append(timestamps, timestamp)
		}
	}
	err = nil
	return
}