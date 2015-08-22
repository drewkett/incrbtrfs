package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
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

func (remote RemoteSnapshotLoc) SendSnapshot(snapshotPath string, parent Timestamp) (err error) {
	var sendErr bytes.Buffer
	var sendCmd *exec.Cmd
	if parent == "" {
		if *verboseFlag {
			log.Println("Performing full send/receive")
		}
		sendCmd = exec.Command(btrfsBin, "send", snapshotPath)
	} else {
		if *verboseFlag {
			log.Println("Performing incremental send/receive")
		}
		parentPath := path.Join(path.Dir(snapshotPath), string(parent))
		sendCmd = exec.Command(btrfsBin, "send", "-p", parentPath, snapshotPath)
	}
	sendCmd.Stderr = &sendErr
	sendOut, err := sendCmd.StdoutPipe()
	if err != nil {
		return
	}

	var receiveOut bytes.Buffer
	var receiveCmd *exec.Cmd
	if remote.Host == "" {
		remoteTarget := path.Join(remote.SnapshotLoc.Directory, "timestamp")
		err = os.MkdirAll(remoteTarget, 0700|os.ModeDir)
		if err != nil {
			return
		}
		receiveCmd = exec.Command(btrfsBin, "receive", remoteTarget)
		receiveCmd.Stdin = sendOut
		receiveCmd.Stdout = &receiveOut
		receiveCmd.Stderr = &receiveOut
	} else {
		sshPath := remote.Host
		if remote.User != "" {
			sshPath = remote.User + "@" + sshPath
		}
		receiveArgs := []string{sshPath, "incrbtrfs", "-receive", remote.SnapshotLoc.Directory, "-timestamp", path.Base(snapshotPath)}
		log.Println(remote.SnapshotLoc.Limits.String())
		if remote.SnapshotLoc.Limits.Hourly > 0 {
			receiveArgs = append(receiveArgs, "-hourly", strconv.Itoa(remote.SnapshotLoc.Limits.Hourly))
		}
		if remote.SnapshotLoc.Limits.Daily > 0 {
			receiveArgs = append(receiveArgs, "-daily", strconv.Itoa(remote.SnapshotLoc.Limits.Daily))
		}
		if remote.SnapshotLoc.Limits.Weekly > 0 {
			receiveArgs = append(receiveArgs, "-weekly", strconv.Itoa(remote.SnapshotLoc.Limits.Weekly))
		}
		if remote.SnapshotLoc.Limits.Monthly > 0 {
			receiveArgs = append(receiveArgs, "-monthly", strconv.Itoa(remote.SnapshotLoc.Limits.Monthly))
		}
		receiveCmd = exec.Command("ssh", receiveArgs...)
		receiveCmd.Stdin = sendOut
		receiveCmd.Stdout = &receiveOut
		receiveCmd.Stderr = &receiveOut
	}

	if *verboseFlag {
		printCommand(sendCmd)
		printCommand(receiveCmd)
	}

	err = receiveCmd.Start()
	if err != nil {
		log.Println("Error with btrfs receive")
		log.Print(receiveOut.String())
		return
	}
	err = sendCmd.Run()
	if err != nil {
		log.Println("Error with btrfs send")
		log.Print(sendErr.String())
		return
	}
	err = receiveCmd.Wait()
	if err != nil {
		log.Println("Error with btrfs receive")
		log.Print(receiveOut.String())
		return
	}
	return
}
