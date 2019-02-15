package main

import (
	"encoding/json"
	"fmt"
	"github.com/golang/snappy"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
)

type RemoteSnapshotsLoc struct {
	Host         string
	Port         string
	User         string
	Exec         string
	SnapshotsLoc SnapshotsLoc
}

func (remote RemoteSnapshotsLoc) GetTimestamps() (timestamps []Timestamp, err error) {
	sshPath := remote.Host
	if remote.User != "" {
		sshPath = remote.User + "@" + sshPath
	}
	var receiveCheckOut []byte
	receiveCheckCmd := exec.Command("ssh", "-C", "-p", remote.Port, sshPath, remote.Exec, "-receive", "-check", "-destination", remote.SnapshotsLoc.Directory)
	if verbosity > 1 {
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

func (remote RemoteSnapshotsLoc) RemoteReceive(in io.Reader, timestamp Timestamp) (retRunner CmdRunner) {
	retRunner = NewCmdRunner()
	go func() {
		if verbosity > 2 {
			log.Println("RemoteReceive")
		}
		if remote.Host == "" {
			err := fmt.Errorf("Invalid command call")
			retRunner.Started <- err
			retRunner.Done <- err
			return
		}
		sshPath := remote.Host
		if remote.User != "" {
			sshPath = remote.User + "@" + sshPath
		}
		receiveArgs := []string{sshPath, "-C", "-p", remote.Port, remote.Exec, "-receive", "-destination", remote.SnapshotsLoc.Directory, "-timestamp", string(timestamp)}
		if verbosity > 2 {
			receiveArgs = append(receiveArgs, "-debug")
		} else if verbosity == 2 {
			receiveArgs = append(receiveArgs, "-verbose")
		} else if verbosity == 0 {
			receiveArgs = append(receiveArgs, "-quiet")
		}
		if *noCompressionFlag {
			receiveArgs = append(receiveArgs, "-noCompression")
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
		if verbosity > 1 {
			printCommand(cmd)
		}
		cmd.Stdin = in
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if verbosity > 2 {
			log.Println("RemoteReceive: Cmd Start")
		}
		err := cmd.Start()
		if err != nil {
			if verbosity > 2 {
				log.Println("RemoteReceive: Cmd Start Error")
			}
			retRunner.Started <- err
			retRunner.Done <- err
			return
		}
		retRunner.Started <- nil
		if verbosity > 2 {
			log.Println("RemoteReceive: Cmd Wait")
		}
		err = cmd.Wait()
		if verbosity > 2 {
			log.Println("RemoteReceive: Cmd Wait Done")
		}
		// signal.Stop(c)
		retRunner.Done <- err
		if verbosity > 2 {
			log.Println("RemoteReceive: End")
		}
	}()
	return
}

func (remote RemoteSnapshotsLoc) SendSnapshot(snapshot Snapshot, parent Timestamp) (err error) {
	var sendCmd *exec.Cmd
	if parent == "" {
		if verbosity > 1 {
			log.Println("Performing full send/receive")
		}
		sendCmd = exec.Command(btrfsBin, "send", snapshot.Path())
	} else {
		if verbosity > 1 {
			log.Println("Performing incremental send/receive")
		}
		parentPath := path.Join(path.Dir(snapshot.Path()), string(parent))
		sendCmd = exec.Command(btrfsBin, "send", "-p", parentPath, snapshot.Path())
	}
	if verbosity > 1 {
		sendCmd.Stderr = os.Stderr
	}
	sendRd, sendWr := io.Pipe()
	defer sendRd.Close()
	var recvRunner CmdRunner
	var compOut *snappy.Writer
	closeComp := false
	if remote.Host == "" {
		sendCmd.Stdout = sendWr
		recvRunner = remote.SnapshotsLoc.ReceiveAndCleanUp(sendRd, snapshot.timestamp)
	} else {
		if *noCompressionFlag {
			sendCmd.Stdout = sendWr
			recvRunner = remote.RemoteReceive(sendRd, snapshot.timestamp)
		} else {
			compOut = snappy.NewBufferedWriter(sendWr)
			closeComp = true
			sendCmd.Stdout = compOut
			recvRunner = remote.RemoteReceive(sendRd, snapshot.timestamp)
		}
	}
	sendRunner := RunCommand(sendCmd)

	err = <-recvRunner.Started
	if err != nil {
		log.Println("Error starting btrfs receive")
		return
	}

	if verbosity > 1 {
		printCommand(sendCmd)
	}
	err = <-sendRunner.Started
	if err != nil {
		log.Println("Error starting btrfs send")
		return
	}
	select {
	case err = <-sendRunner.Done:
		if err != nil {
			log.Println("Error running btrfs send")
		}
		if closeComp {
			compOut.Close()
		}
		sendWr.Close()
		<-recvRunner.Done
		return
	case err = <-recvRunner.Done:
		if err != nil {
			log.Println("Error running btrfs receive")
		}
		sendRunner.Signal <- os.Kill
		<-sendRunner.Done
		return
	}
}

func (remote RemoteSnapshotsLoc) sendSnapshotUsingParent(localSnapshot Snapshot, localTimestamps []Timestamp) (err error) {
	var remoteTimestamps []Timestamp
	var lock DirLock
	if remote.Host == "" {
		lock, err = NewDirLock(remote.SnapshotsLoc.Directory)
		if err != nil {
			return
		}
		defer lock.Unlock()
		remoteTimestamps, err = remote.SnapshotsLoc.ReadTimestampsDir()
		if err != nil {
			return
		}
	} else {
		remoteTimestamps, err = remote.GetTimestamps()
		if err != nil {
			return
		}
	}
	parentTimestamp := calcParent(localTimestamps, remoteTimestamps)
	if verbosity > 0 && parentTimestamp != "" {
		log.Printf("Parent = %s\n", string(parentTimestamp))
	}
	err = remote.SendSnapshot(localSnapshot, parentTimestamp)
	return
}
