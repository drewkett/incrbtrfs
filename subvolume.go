package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
)

type Subvolume struct {
	Directory         string
	SnapshotDirectory string
	Limits            Limits
	Remotes           []SubvolumeRemote
}

func (subvolume *Subvolume) Print() {
	fmt.Printf("Snapshot Dir='%s' (%s)\n", subvolume.Directory, subvolume.Limits.String())
	for _, remote := range subvolume.Remotes {
		dst := remote.Directory
		if remote.Host != "" {
			dst = strings.Join([]string{remote.Host, dst}, ":")
			if remote.User != "" {
				dst = strings.Join([]string{remote.User, dst}, "@")
			}
		}
		fmt.Printf("Remote Dir='%s' (%s)\n", dst, remote.Limits.String())
	}

}

func (subvolume *Subvolume) clean(interval Interval, now time.Time, timestamps []Timestamp) (keptTimestamps TimestampMap, err error) {
	dir := path.Join(subvolume.SnapshotDirectory, string(interval))
	err = os.MkdirAll(dir, os.ModeDir|0700)
	if err != nil {
		return
	}
	err = removeAllSymlinks(dir)
	if err != nil {
		return
	}
	maxIndex := interval.GetMaxIndex(subvolume.Limits)
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
			fmt.Printf("Symlink '%s' => '%s'\n", dst, src)
		}
		err = os.Symlink(src, dst)
		if err != nil {
			return
		}
	}
	return
}

func (subvolume *Subvolume) cleanUp(nowTimestamp Timestamp, timestamps []Timestamp) (err error) {
	now, err := parseTimestamp(nowTimestamp)
	if err != nil {
		return
	}
	timestampsDir := path.Join(subvolume.SnapshotDirectory, "timestamp")
	keptTimestamps := make(TimestampMap)
	keptTimestamps[nowTimestamp] = true
	var tempMap TimestampMap
	for _, interval := range Intervals {
		tempMap, err = subvolume.clean(interval, now, timestamps)
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
			output, err = btrfsCmd.CombinedOutput()
			if err != nil {
				if !(*quietFlag) {
					fmt.Printf("%s", output)
				}
				return
			}
		}
	}
	return
}

func (subvolume *Subvolume) receiveSnapshot(timestamp Timestamp) (err error) {
	targetPath := path.Join(subvolume.SnapshotDirectory, "timestamp")
	receiveCmd := exec.Command(btrfsBin, "receive", targetPath)
	receiveCmd.Stdin = os.Stdin
	receiveOut, err := receiveCmd.CombinedOutput()
	if err != nil {
		fmt.Print(receiveOut)
		if _, errTmp := os.Stat(targetPath); !os.IsNotExist(errTmp) {
			errTmp = DeleteSnapshot(targetPath)
			if errTmp != nil {
				if !(*quietFlag) {
					fmt.Println("Failed to deleted to failed snapshot")
				}
			}
		}
		return
	}
	timestamps, err := readTimestampsDir(subvolume.SnapshotDirectory)
	if err != nil {
		return
	}
	err = subvolume.cleanUp(timestamp, timestamps)
	if err != nil {
		return
	}
	return
}
