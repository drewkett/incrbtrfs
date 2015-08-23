package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

//TODO add comments

const btrfsBin string = "/sbin/btrfs"
const subDir string = ".incrbtrfs"
const timeFormat string = "20060102_150405"
const version int = 2

var quietFlag = flag.Bool("quiet", false, "Quiet Mode")
var verboseFlag = flag.Bool("verbose", false, "Verbose Mode")
var debugFlag = flag.Bool("debug", false, "Debug Mode")
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
	var snapshotsLoc SnapshotsLoc
	snapshotsLoc.Directory = *receiveFlag
	snapshotsLoc.Limits = Limits{
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
	cw := NewCmdWatcher()
	if *debugFlag {
		log.Println("runRemote: ReceiveAndCleanUp")
	}
	go snapshotsLoc.ReceiveAndCleanUp(os.Stdin, timestamp, cw)
	err = <-cw.Started
	if *debugFlag {
		log.Println("runRemote: ReceiveAndCleanUp Started")
	}
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	err = <-cw.Done
	if *debugFlag {
		log.Println("runRemote: ReceiveAndCleanUp Done")
	}
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

func setRemoteLogging() {
	log.SetPrefix("[remote] ")
}

func main() {
	err := getLock()
	if err != nil {
		log.Println("Error acquiring local lock")
		os.Exit(1)
	}
	setLoggingDefaults()

	flag.Parse()

	if *debugFlag {
		*verboseFlag = true
		*quietFlag = false
	} else if *verboseFlag {
		*quietFlag = false
	}

	if *receiveCheckFlag != "" {
		setRemoteLogging()
		runRemoteCheck()
	} else if *receiveFlag != "" {
		setRemoteLogging()
		runRemote()
	} else {
		runLocal()
	}
}
