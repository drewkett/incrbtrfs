package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
)

type RemoteSnapshotLoc struct {
	Host        string
	User        string
	SnapshotLoc SnapshotLoc
}

func (remote RemoteSnapshotLoc) GetTimestamps() (timestamps []Timestamp, err error) {
	sshPath := remote.Host
	if remote.User != "" {
		sshPath = remote.User + "@" + sshPath
	}
	var receiveCheckOut []byte
	var receiveCheckErr bytes.Buffer
	receiveCheckCmd := exec.Command("ssh", sshPath, "incrbtrfs", "-receiveCheck", remote.SnapshotLoc.Directory)
	receiveCheckCmd.Stderr = &receiveCheckErr
	receiveCheckOut, err = receiveCheckCmd.Output()
	if err != nil {
		log.Println("Failed to run ReceiveCheck")
		log.Println(string(receiveCheckOut))
		log.Println(receiveCheckErr.String())
		return
	}
	log.Println(string(receiveCheckOut))
	var checkStr RemoteCheck
	err = json.Unmarshal(receiveCheckOut, &checkStr)
	if err != nil {
		log.Println("Failed to read ReceiveCheck JSON")
		return
	}
	if checkStr.Version != version {
		err = fmt.Errorf("Incompatible Version Local (%d) != Remote (%d)", version, checkStr.Version)
		return
	}
	for _, timestampStr := range checkStr.Timestamps {
		timestamp := Timestamp(timestampStr)
		_, err := parseTimestamp(timestamp)
		if err != nil {
			continue
		}
		timestamps = append(timestamps, timestamp)
	}
	return
}
