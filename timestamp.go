package main

import (
	"io/ioutil"
	"os"
	"path"
	"time"
)

type Timestamp string
type Timestamps []Timestamp
type TimestampMap map[Timestamp]bool

func (p Timestamps) Len() int           { return len(p) }
func (p Timestamps) Less(i, j int) bool { return string(p[i]) < string(p[j]) }
func (p Timestamps) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (tm TimestampMap) Merge(other TimestampMap) TimestampMap {
	for key, _ := range other {
		tm[key] = true
	}
	return tm
}

func parseTimestamp(timestamp Timestamp) (t time.Time, err error) {
	t, err = time.ParseInLocation(timeFormat, string(timestamp), time.Local)
	return
}
func readTimestampsDir(snapshotsDir string) (timestamps []Timestamp, err error) {
	timestampsDir := path.Join(snapshotsDir, "timestamp")
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
