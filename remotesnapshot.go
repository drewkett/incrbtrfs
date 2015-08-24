package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strconv"
	"syscall"
)

type RemoteSnapshotsLoc struct {
	Host         string
	User         string
	SnapshotsLoc SnapshotsLoc
}

func (remote RemoteSnapshotsLoc) GetTimestamps() (timestamps []Timestamp, err error) {
	sshPath := remote.Host
	if remote.User != "" {
		sshPath = remote.User + "@" + sshPath
	}
	var receiveCheckOut []byte
	receiveCheckCmd := exec.Command("ssh", sshPath, "incrbtrfs", "-receiveCheck", remote.SnapshotsLoc.Directory)
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

func PassSignal(cmd *exec.Cmd, c chan os.Signal) {
	for {
		sig := <-c
		cmd.Process.Signal(sig)
	}
}

func (remote RemoteSnapshotsLoc) RemoteReceive(in io.Reader, timestamp Timestamp, cw CmdWatcher) {
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
	receiveArgs := []string{sshPath, "incrbtrfs", "-receive", remote.SnapshotsLoc.Directory, "-timestamp", string(timestamp)}
	if *debugFlag {
		receiveArgs = append(receiveArgs, "-debug")
	} else if *verboseFlag {
		receiveArgs = append(receiveArgs, "-verbose")
	} else if *quietFlag {
		receiveArgs = append(receiveArgs, "-quiet")
	}
	if remote.SnapshotsLoc.Limits.Hourly > 0 {
		receiveArgs = append(receiveArgs, "-hourly", strconv.Itoa(remote.SnapshotsLoc.Limits.Hourly))
	}
	if remote.SnapshotsLoc.Limits.Daily > 0 {
		receiveArgs = append(receiveArgs, "-daily", strconv.Itoa(remote.SnapshotsLoc.Limits.Daily))
	}
	if remote.SnapshotsLoc.Limits.Weekly > 0 {
		receiveArgs = append(receiveArgs, "-weekly", strconv.Itoa(remote.SnapshotsLoc.Limits.Weekly))
	}
	if remote.SnapshotsLoc.Limits.Monthly > 0 {
		receiveArgs = append(receiveArgs, "-monthly", strconv.Itoa(remote.SnapshotsLoc.Limits.Monthly))
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
	c := make(chan os.Signal, 1)
	// go PassSignal(cmd, c)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGINT)
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
		log.Println("RemoteReceive: Cmd Wait Done")
	}
	signal.Stop(c)
	cw.Done <- err
}

func (remote RemoteSnapshotsLoc) SendSnapshot(snapshot Snapshot, parent Timestamp) (err error) {
	var sendErr bytes.Buffer
	var sendCmd *exec.Cmd
	if parent == "" {
		if *verboseFlag {
			log.Println("Performing full send/receive")
		}
		sendCmd = exec.Command(btrfsBin, "send", snapshot.Path())
	} else {
		if *verboseFlag {
			log.Println("Performing incremental send/receive")
		}
		parentPath := path.Join(path.Dir(snapshot.Path()), string(parent))
		sendCmd = exec.Command(btrfsBin, "send", "-p", parentPath, snapshot.Path())
	}
	sendCmd.Stderr = &sendErr
	sendOut, err := sendCmd.StdoutPipe()
	if err != nil {
		return
	}

	cw := NewCmdWatcher()
	if remote.Host == "" {
		go remote.SnapshotsLoc.ReceiveAndCleanUp(sendOut, snapshot.timestamp, cw)
	} else {
		go remote.RemoteReceive(sendOut, snapshot.timestamp, cw)
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
