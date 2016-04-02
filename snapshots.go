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
	err = os.MkdirAll(dir, dirMode)
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
		if verbosity > 1 {
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
	if verbosity > 2 {
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

func (snapshotsLoc SnapshotsLoc) ReceiveSnapshot(in io.Reader, timestamp Timestamp) (retRunner CmdRunner) {
	retRunner = NewCmdRunner()
	go func() {
		if verbosity > 2 {
			log.Println("ReceiveSnapshot")
		}
		targetPath := path.Join(snapshotsLoc.Directory, "timestamp")
		err := os.MkdirAll(targetPath, dirMode)
		if verbosity > 2 {
			log.Println("ReceiveSnapshot: MkdirAll")
		}
		if err != nil {
			retRunner.Started <- err
			retRunner.Done <- err
			return
		}
		receiveCmd := exec.Command(btrfsBin, "receive", targetPath)
		if verbosity > 1 {
			printCommand(receiveCmd)
		}
		receiveCmd.Stdin = in
		if verbosity > 1 {
			receiveCmd.Stdout = os.Stderr
			receiveCmd.Stderr = os.Stderr
		}
		runner := RunCommand(receiveCmd)
		err = <-runner.Started
		if verbosity > 2 {
			log.Println("ReceiveSnapshot: Cmd Started")
		}
		if err != nil {
			retRunner.Started <- err
			retRunner.Done <- err
			return
		}
		retRunner.Started <- nil
		if verbosity > 2 {
			log.Println("ReceiveSnapshot: Cmd Sent Started")
		}
		err = <-runner.Done
		if verbosity > 2 {
			log.Println("ReceiveSnapshot: Cmd Wait")
		}
		if err != nil {
			if verbosity > 2 {
				log.Printf("ReceiveSnapshot: Error '%s'", err.Error())
			}
			snapshot := Snapshot{snapshotsLoc, timestamp}
			if _, errTmp := os.Stat(snapshot.Path()); !os.IsNotExist(errTmp) {
				errTmp = snapshot.DeleteSnapshot()
			}
		}
		retRunner.Done <- err
	}()
	return
}

func (snapshotsLoc SnapshotsLoc) ReceiveAndCleanUp(in io.Reader, timestamp Timestamp) (retRunner CmdRunner) {
	retRunner = NewCmdRunner()
	go func() {
		if verbosity > 2 {
			log.Println("ReceiveAndCleanup")
		}
		runner := snapshotsLoc.ReceiveSnapshot(in, timestamp)
		if verbosity > 2 {
			log.Println("ReceiveAndCleanup: ReceiveSnapshot")
		}
		err := <-runner.Started
		if verbosity > 2 {
			log.Println("ReceiveAndCleanup: Receive Started")
		}
		if err != nil {
			retRunner.Started <- err
			retRunner.Done <- err
			return
		}
		if verbosity > 2 {
			log.Println("ReceiveAndCleanup: Started")
		}
		retRunner.Started <- nil
		if verbosity > 2 {
			log.Println("ReceiveAndCleanup: Sent Started")
		}
		err = <-runner.Done
		if verbosity > 2 {
			log.Println("ReceiveAndCleanup: Receive Done")
		}
		if err != nil {
			retRunner.Done <- err
			return
		}
		if verbosity > 2 {
			log.Println("ReceiveAndCleanup: ReadTimestampsDir")
		}
		timestamps, err := snapshotsLoc.ReadTimestampsDir()
		if err != nil {
			retRunner.Done <- err
			return
		}
		if verbosity > 2 {
			log.Println("ReceiveAndCleanup: CleanUp")
		}
		_, err = snapshotsLoc.CleanUp(timestamp, timestamps)
		retRunner.Done <- err
	}()
	return
}

func (snapshotsLoc SnapshotsLoc) ReadTimestampsDir() (timestamps []Timestamp, err error) {
	timestampsDir := path.Join(snapshotsLoc.Directory, "timestamp")
	err = os.MkdirAll(timestampsDir, dirMode)
	if err != nil {
		return
	}
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
