package main

import (
	"encoding/json"
	"flag"
	"github.com/golang/snappy"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

//TODO add comments
//TODO check signal handling
//TODO create file signifying successful snapshots
//TODO make .incrbtrfs directory a subvolume. Prevents future snapshots from
//including directory by default.
//TODO Require shared lock to delete subvolume. Use shared locks when running send operation.
//allows incrbtrfs to be run again while long running operation is running
//TODO delete archive file on interrupt

const btrfsBin string = "btrfs"
const subDir string = ".incrbtrfs"
const timeFormat string = "20060102_150405"
const version int = 3
const dirMode os.FileMode = 0700 | os.ModeDir

var quietFlag = flag.Bool("quiet", false, "Quiet Mode")
var verboseFlag = flag.Bool("verbose", false, "Verbose Mode")
var debugFlag = flag.Bool("debug", false, "Debug Mode")
var destinationFlag = flag.String("destination", "", "Destination directory for -receive")
var checkFlag = flag.Bool("check", false, "Activate Check Mode for -receive")
var receiveFlag = flag.Bool("receive", false, "Receive Mode")
var loadFileFlag = flag.String("loadFile", "", "Load Snapshot File")
var timestampFlag = flag.String("timestamp", "", "Timestamp for Receive Mode")
var hourlyFlag = flag.Int("hourly", 0, "Hourly Limit")
var dailyFlag = flag.Int("daily", 0, "Daily Limit")
var weeklyFlag = flag.Int("weekly", 0, "Weekly Limit")
var monthlyFlag = flag.Int("monthly", 0, "Monthly Limit")
var pinnedFlag = flag.Bool("pin", false, "Keep snapshots indefinitely")
var archiveFlag = flag.Bool("archive", false, "Create archive file of snapshots (implies -pin)")
var noCompressionFlag = flag.Bool("noCompression", false, "Disable compression for btrfs send/receive and -archive")

var verbosity = 1

func printCommand(cmd *exec.Cmd) {
	log.Printf("Running '%s %s'\n", cmd.Path, strings.Join(cmd.Args[1:], " "))
}

func getCurrentTimestamp() Timestamp {
	currentTime := time.Now()
	return Timestamp(currentTime.Format(timeFormat))
}

func runLoadFile() {
	if *destinationFlag == "" {
		log.Println("Must specify destination in loadFile mode")
		os.Exit(1)
	}
	var snapshotsLoc SnapshotsLoc
	snapshotsLoc.Directory = *destinationFlag
	snapshotsLoc.Limits = Limits{
		Hourly:  *hourlyFlag,
		Daily:   *dailyFlag,
		Weekly:  *weeklyFlag,
		Monthly: *monthlyFlag}

	lock, err := NewDirLock(snapshotsLoc.Directory)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	defer lock.Unlock()
	fileName := *loadFileFlag
	baseName := path.Base(fileName)
	var timestampStr string
	var compressed bool
	if strings.HasSuffix(baseName, ".snap.snpy") {
		timestampStr = strings.TrimSuffix(baseName, ".snap.snpy")
		compressed = true
	} else if strings.HasSuffix(baseName, ".snap") {
		timestampStr = strings.TrimSuffix(baseName, ".snap")
		compressed = false
	} else {
		log.Printf("Unrecognized file type for %s", baseName)
		os.Exit(1)
	}
	f, err := os.Open(fileName)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	defer f.Close()

	timestamp := Timestamp(timestampStr)
	var runner CmdRunner
	if compressed {
		cf := snappy.NewReader(f)
		runner = snapshotsLoc.ReceiveSnapshot(cf, timestamp)
	} else {
		runner = snapshotsLoc.ReceiveSnapshot(f, timestamp)
	}
	err = <-runner.Started
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	err = <-runner.Done
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	if *pinnedFlag {
		err = snapshotsLoc.PinTimestamp(timestamp)
		if err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
	}
}

type RemoteCheck struct {
	Version    int
	Timestamps []string
}

func runRemoteCheck() {
	if *destinationFlag == "" {
		log.Println("Must specify destination in receive-check mode")
		os.Exit(1)
	}
	recvDir := *destinationFlag
	lock, err := NewDirLock(recvDir)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	defer lock.Unlock()
	timestampsDir := path.Join(recvDir, "timestamp")
	err = os.MkdirAll(timestampsDir, dirMode)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	fis, err := ioutil.ReadDir(timestampsDir)
	if err != nil {
		log.Println(err.Error())
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
		log.Println(err.Error())
		os.Exit(1)
	}
	n, err := os.Stdout.Write(data)
	if err != nil || n != len(data) {
		log.Println(err.Error())
		os.Exit(1)
	}
}

func runRemote() {
	if *destinationFlag == "" {
		log.Println("Must specify destination in receive mode")
		os.Exit(1)
	}
	if *timestampFlag == "" {
		log.Println("Must specify timestamp in receive mode")
		os.Exit(1)
	}
	var snapshotsLoc SnapshotsLoc
	snapshotsLoc.Directory = *destinationFlag
	snapshotsLoc.Limits = Limits{
		Hourly:  *hourlyFlag,
		Daily:   *dailyFlag,
		Weekly:  *weeklyFlag,
		Monthly: *monthlyFlag}

	lock, err := NewDirLock(snapshotsLoc.Directory)
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer lock.Unlock()
	timestamp := Timestamp(*timestampFlag)
	_, err = parseTimestamp(timestamp)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	if verbosity > 2 {
		log.Println("runRemote: ReceiveAndCleanUp")
	}
	var runner CmdRunner
	if *noCompressionFlag {
		runner = snapshotsLoc.ReceiveAndCleanUp(os.Stdin, timestamp)
	} else {
		rd := snappy.NewReader(os.Stdin)
		runner = snapshotsLoc.ReceiveAndCleanUp(rd, timestamp)
	}
	err = <-runner.Started
	if verbosity > 2 {
		log.Println("runRemote: ReceiveAndCleanUp Started")
	}
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	err = <-runner.Done
	if verbosity > 2 {
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
	config, err := parseFile(flag.Arg(0))
	if err != nil {
		log.Println("Erroring parsing file")
		log.Println(err.Error())
		os.Exit(1)
	}
	subvolumes := parseConfig(config)
	isErr := false
	for _, subvolume := range subvolumes {
		if verbosity > 0 {
			subvolume.Print()
		}
		err = subvolume.RunSnapshot()
		if err != nil {
			log.Println(err)
			isErr = true
		}
	}
	if isErr {
		os.Exit(1)
	}

}

func setLoggingDefaults() {
	log.SetOutput(os.Stderr)
	log.SetFlags(0)
}

func setRemoteLogging() {
	log.SetPrefix("[remote] ")
}

func main() {
	setLoggingDefaults()

	flag.Parse()

	if *debugFlag {
		verbosity = 3
	} else if *verboseFlag {
		verbosity = 2
	} else if *quietFlag {
		verbosity = 0
	}

	if *archiveFlag {
		*pinnedFlag = true
	}

	if *loadFileFlag != "" {
		runLoadFile()
	} else if *receiveFlag && *checkFlag {
		setRemoteLogging()
		runRemoteCheck()
	} else if *receiveFlag {
		setRemoteLogging()
		runRemote()
	} else {
		runLocal()
	}
}
