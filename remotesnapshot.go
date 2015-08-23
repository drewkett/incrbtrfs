package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	receiveCheckCmd := exec.Command("ssh", sshPath, "incrbtrfs", "-receiveCheck", remote.SnapshotLoc.Directory)
	if *verboseFlag {
		receiveCheckCmd.Stderr = os.Stderr
	}
	receiveCheckOut, err = receiveCheckCmd.Output()
	if err != nil {
		log.Println("Failed to run ReceiveCheck")
		log.Println(string(receiveCheckOut))
		return
	}
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

func (remote RemoteSnapshotLoc) RemoteReceive(in io.Reader, timestamp Timestamp, cw CmdWatcher) {
	if *debugFlag {
		log.Println("RemoteReceive")
	}
	if remote.Host == "" {
		err := fmt.Errorf("Invalid command call")
		cw.Started <- err
		cw.Done <- err
		return
	}
	sshPath := remote.Host
	if remote.User != "" {
		sshPath = remote.User + "@" + sshPath
	}
	receiveArgs := []string{sshPath, "incrbtrfs", "-receive", remote.SnapshotLoc.Directory, "-timestamp", string(timestamp)}
	if *debugFlag {
		receiveArgs = append(receiveArgs, "-debug")
	} else if *verboseFlag {
		receiveArgs = append(receiveArgs, "-verbose")
	} else if *quietFlag {
		receiveArgs = append(receiveArgs, "-quiet")
	}
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
	cmd := exec.Command("ssh", receiveArgs...)
	if *verboseFlag {
		printCommand(cmd)
	}
	cmd.Stdin = in
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if *debugFlag {
		log.Println("RemoteReceive: Cmd Start")
	}
	err := cmd.Start()
	if err != nil {
		if *debugFlag {
			log.Println("RemoteReceive: Cmd Start Error")
		}
		cw.Started <- err
		cw.Done <- err
		return
	}
	cw.Started <- nil
	if *debugFlag {
		log.Println("RemoteReceive: Cmd Wait")
	}
	err = cmd.Wait()
	if *debugFlag {
		log.Println("RemoteReceive: Cmd Wait")
	}
	cw.Done <- err
}

func (remote RemoteSnapshotLoc) SendSnapshot(timestampPath string, timestamp Timestamp, parent Timestamp) (err error) {
	snapshotPath := path.Join(timestampPath, string(timestamp))
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

	cw := NewCmdWatcher()
	if remote.Host == "" {
		go remote.SnapshotLoc.ReceiveAndCleanUp(sendOut, timestamp, cw)
	} else {
		go remote.RemoteReceive(sendOut, timestamp, cw)
	}

	err = <-cw.Started
	if err != nil {
		log.Println("Error with btrfs receive")
		return
	}

	if *verboseFlag {
		printCommand(sendCmd)
	}
	err = sendCmd.Run()
	if err != nil {
		log.Println("Error with btrfs send")
		log.Print(sendErr.String())
		return
	}
	err = <-cw.Done
	if err != nil {
		log.Println("Error with btrfs receive")
		return
	}
	return
}
