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

type SnapshotLoc struct {
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

func (snapshotLoc SnapshotLoc) clean(interval Interval, now time.Time, timestamps []Timestamp) (keptTimestamps TimestampMap, err error) {
	dir := path.Join(snapshotLoc.Directory, string(interval))
	err = os.MkdirAll(dir, os.ModeDir|0700)
	if err != nil {
		return
	}
	err = removeAllSymlinks(dir)
	if err != nil {
		return
	}
	maxIndex := interval.GetMaxIndex(snapshotLoc.Limits)
	keptIndices := make(map[int]bool)
	keptTimestamps = make(TimestampMap)
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
		keptTimestamps[timestamp] = true
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

func (snapshotLoc SnapshotLoc) CleanUp(nowTimestamp Timestamp, timestamps []Timestamp) (err error) {
	if *verboseFlag {
		log.Println("Running Clean Up")
	}
	now, err := parseTimestamp(nowTimestamp)
	if err != nil {
		return
	}
	timestampsDir := path.Join(snapshotLoc.Directory, "timestamp")
	keptTimestamps := make(TimestampMap)
	keptTimestamps[nowTimestamp] = true
	var tempMap TimestampMap
	for _, interval := range Intervals {
		tempMap, err = snapshotLoc.clean(interval, now, timestamps)
		if err != nil {
			return
		}
		keptTimestamps = keptTimestamps.Merge(tempMap)
	}
	// Remove unneeded timestamps
	for _, timestamp := range timestamps {
		if _, ok := keptTimestamps[timestamp]; !ok {
			var output []byte
			timestampLoc := path.Join(timestampsDir, string(timestamp))
			btrfsCmd := exec.Command(btrfsBin, "subvolume", "delete", timestampLoc)
			if *verboseFlag {
				printCommand(btrfsCmd)
			}
			output, err = btrfsCmd.CombinedOutput()
			if err != nil {
				if !(*quietFlag) {
					log.Printf("%s", output)
				}
				return
			}
		}
	}
	return
}

func (snapshotLoc SnapshotLoc) ReceiveAndCleanUp(in io.Reader, timestamp Timestamp, cw CmdWatcher) {
	targetPath := path.Join(snapshotLoc.Directory, "timestamp")
	subCW := NewCmdWatcher()
	go ReceiveSnapshot(in, targetPath, subCW)
	err := <-subCW.Started
	if err != nil {
		cw.Started <- err
		cw.Done <- err
		return
	}
	cw.Started <- nil
	err = <-subCW.Done
	if err != nil {
		cw.Done <- err
		return
	}
	timestamps, err := snapshotLoc.ReadTimestampsDir()
	if err != nil {
		cw.Done <- err
		return
	}
	err = snapshotLoc.CleanUp(timestamp, timestamps)
	cw.Done <- err
	return
}

func (snapshotLoc SnapshotLoc) ReadTimestampsDir() (timestamps []Timestamp, err error) {
	timestampsDir := path.Join(snapshotLoc.Directory, "timestamp")
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
