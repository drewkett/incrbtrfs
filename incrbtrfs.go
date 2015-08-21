package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
)

//TODO make sure to delete failed send/receives
//TODO add comments

const btrfsBin string = "/sbin/btrfs"
const subDir string = ".incrbtrfs"
const timeFormat string = "20060102_150405"
const version int = 2

var quietFlag = flag.Bool("quiet", false, "Quiet Mode")
var verboseFlag = flag.Bool("verbose", false, "Verbose Mode")
var receiveCheckFlag = flag.String("receiveCheck", "", "Receive Mode (Check)")
var receiveFlag = flag.String("receive", "", "Receive Mode")
var timestampFlag = flag.String("timestamp", "", "Timestamp for Receive Mode")
var hourlyFlag = flag.Int("hourly", 0, "Hourly Limit")
var dailyFlag = flag.Int("daily", 0, "Daily Limit")
var weeklyFlag = flag.Int("weekly", 0, "Weekly Limit")
var monthlyFlag = flag.Int("monthly", 0, "Monthly Limit")

func printCommand(cmd *exec.Cmd) {
	log.Printf("Running '%s %s'\n", cmd.Path, strings.Join(cmd.Args[1:], " "))
}

func sendSnapshot(snapshotPath string, remote RemoteSnapshotLoc, parent Timestamp) (err error) {
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
		// var receiveCheckOut []byte
		// var receiveCheckErr bytes.Buffer
		// receiveCheckCmd := exec.Command("ssh", sshPath, "incrbtrfs", "-receiveCheck", remote.Directory)
		// receiveCheckCmd.Stderr = &receiveCheckErr
		// receiveCheckOut, err = receiveCheckCmd.Output()
		// if err != nil {
		// 	log.Println("Failed to run ReceiveCheck")
		// 	log.Println(string(receiveCheckOut))
		// 	log.Println(receiveCheckErr.String())
		// 	return
		// }
		// var checkStr RemoteCheck
		// err = json.Unmarshal(receiveCheckOut, &checkStr)
		// if err != nil {
		// 	log.Println("Failed to read ReceiveCheck JSON")
		// 	return
		// }
		// if checkStr.Version != version {
		// 	err = log.Errorf("Incompatible Version Local (%d) != Remote (%d)", version, checkStr.Version)
		// 	return
		// }
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

func getCurrentTimestamp() Timestamp {
	currentTime := time.Now()
	return Timestamp(currentTime.Format(timeFormat))
}

type RemoteCheck struct {
	Version    int
	Timestamps []string
}

func runRemoteCheck() {
	timestampsDir := path.Join(*receiveCheckFlag, "timestamp")
	err := os.MkdirAll(timestampsDir, 0700|os.ModeDir)
	if err != nil {
		log.Print(err.Error())
		os.Exit(1)
	}
	fis, err := ioutil.ReadDir(timestampsDir)
	if err != nil {
		log.Print(err.Error())
		os.Exit(1)
	}
	var checkStr RemoteCheck
	checkStr.Version = version
	checkStr.Timestamps = make([]string, 0)
	for _, fi := range fis {
		if fi.IsDir() {
			timestamp := Timestamp(fi.Name())
			_, err := parseTimestamp(timestamp)
			if err != nil {
				continue
			}
			checkStr.Timestamps = append(checkStr.Timestamps, string(timestamp))
		}
	}
	data, err := json.Marshal(checkStr)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
	n, err := os.Stdout.Write(data)
	if err != nil || n != len(data) {
		log.Print(err)
		os.Exit(1)
	}
}

func runRemote() {
	if *timestampFlag == "" {
		log.Println("Must specify timestamp in receive mode")
		os.Exit(1)
	}
	var snapshotLoc SnapshotLoc
	snapshotLoc.Directory = *receiveFlag
	snapshotLoc.Limits = Limits{
		Hourly:  *hourlyFlag,
		Daily:   *dailyFlag,
		Weekly:  *weeklyFlag,
		Monthly: *monthlyFlag}
	timestamp := Timestamp(*timestampFlag)
	_, err := parseTimestamp(timestamp)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	err = snapshotLoc.ReceiveSnapshot(timestamp)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
}

func runLocal() {
	if flag.NArg() != 1 {
		log.Println("One Argument Required")
		os.Exit(1)
	}
	currentTimestamp := getCurrentTimestamp()
	config, err := parseFile(flag.Arg(0))
	if err != nil {
		log.Println("Erroring parsing file")
		log.Println(err.Error())
		os.Exit(1)
	}
	subvolumes := parseConfig(config)
	isErr := false
	for _, subvolume := range subvolumes {
		if !(*quietFlag) {
			subvolume.Print()
		}
		err = subvolume.RunSnapshot(currentTimestamp)
		if err != nil {
			log.Println(err)
			isErr = true
		}
	}
	if isErr {
		os.Exit(1)
	}

}

func getLock() (err error) {
	_, err = net.Listen("unix", "@incrbtrfs")
	return
}

func setLoggingDefaults() {
	log.SetOutput(os.Stderr)
	log.SetFlags(0)
}

func main() {
	err := getLock()
	if err != nil {
		log.Println("Error acquiring local lock")
		os.Exit(1)
	}
	setLoggingDefaults()

	flag.Parse()
	if *receiveCheckFlag != "" {
		runRemoteCheck()
	} else if *receiveFlag != "" {
		runRemote()
	} else {
		runLocal()
	}
}
