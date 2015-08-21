package main

import (
	"sort"
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

func calcParent(localTimestamps []Timestamp, remoteTimestamps []Timestamp) Timestamp {
	timestampMap := make(TimestampMap)
	for _, timestamp := range localTimestamps {
		timestampMap[timestamp] = true
	}
	sort.Sort(sort.Reverse(Timestamps(remoteTimestamps)))
	for _, remoteTimestamp := range remoteTimestamps {
		if _, ok := timestampMap[remoteTimestamp]; ok {
			return remoteTimestamp
		}
	}
	return ""
}
