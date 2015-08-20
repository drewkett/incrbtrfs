package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
)

//TODO implement incremntal backups
//TODO make sure to delete failed send/receives
//TODO add comments
const btrfsBin string = "/sbin/btrfs"
const subDir string = ".incrbtrfs"
const timeFormat string = "20060102_150405"

var Intervals = [...]string{"hourly", "daily", "weekly", "monthly"}
var quietFlag = flag.Bool("quiet", false, "Quiet Mode")
var verboseFlag = flag.Bool("verbose", false, "Verbose Mode")
var receiveFlag = flag.String("receive", "", "Receive Mode")
var timestampFlag = flag.String("timestamp", "", "Timestamp for Receive Mode")
var hourlyFlag = flag.Int("hourly", 0, "Hourly Limit")
var dailyFlag = flag.Int("daily", 0, "Daily Limit")
var weeklyFlag = flag.Int("weekly", 0, "Weekly Limit")
var monthlyFlag = flag.Int("monthly", 0, "Monthly Limit")

type Limits struct {
	Hourly  int
	Daily   int
	Weekly  int
	Monthly int
}

func (l Limits) String() string {
	return fmt.Sprintf("Hourly=%d, Daily=%d, Weekly=%d, Monthly=%d", l.Hourly, l.Daily, l.Weekly, l.Monthly)
}

type SubvolumeRemote struct {
	Host      string
	User      string
	Directory string
	Limits    Limits
}

type Subvolume struct {
	Directory string
	Limits    Limits
	Remotes   []SubvolumeRemote
}

type Timestamp struct {
	string string
	time   time.Time
}

type TimestampMap map[string]bool

func (subvolume *Subvolume) Print() {
	fmt.Printf("Snapshot Dir='%s' (%s)\n", subvolume.Directory, subvolume.Limits.String())
	for _, remote := range subvolume.Remotes {
		dst := remote.Directory
		if remote.Host != "" {
			dst = strings.Join([]string{remote.Host, dst}, ":")
			if remote.User != "" {
				dst = strings.Join([]string{remote.User, dst}, "@")
			}
		}
		fmt.Printf("Remote Dir='%s' (%s)\n", dst, remote.Limits.String())
	}

}

func combineLimits(limits Limits, newLimits ...OptionalLimits) (updateLimits Limits) {
	updateLimits = limits
	for _, l := range newLimits {
		if l.Hourly != nil {
			updateLimits.Hourly = *l.Hourly
		}
		if l.Daily != nil {
			updateLimits.Daily = *l.Daily
		}
		if l.Weekly != nil {
			updateLimits.Weekly = *l.Weekly
		}
		if l.Monthly != nil {
			updateLimits.Monthly = *l.Monthly
		}
	}
	return
}

func timestampFromString(timestampString string) (timestamp Timestamp, err error) {
	t, err := time.ParseInLocation(timeFormat, timestampString, time.Local)
	if err != nil {
		return
	}
	timestamp = Timestamp{string: timestampString, time: t}
	return
}

func readTimestampDir(timestampDir string) (timestamps []Timestamp, err error) {
	fileInfos, err := ioutil.ReadDir(timestampDir)
	if err != nil {
		return
	}
	for _, fi := range fileInfos {
		if fi.IsDir() {
			timestamp, err := timestampFromString(fi.Name())
			if err != nil {
				continue
			}
			timestamps = append(timestamps, timestamp)
		}
	}
	err = nil
	return
}

func calcIndex(now time.Time, snapshotTime time.Time, interval string) (i int, err error) {
	firstMonday := time.Date(1970, 1, 5, 0, 0, 0, 0, time.UTC)
	switch interval {
	case "hourly":
		now = now.Truncate(time.Hour)
		snapshotTime = snapshotTime.Truncate(time.Hour)
		i = int(now.Sub(snapshotTime).Hours())
	case "daily":
		nowDays := int(now.Sub(firstMonday).Hours() / 24)
		snapshotDays := int(snapshotTime.Sub(firstMonday).Hours() / 24)
		i = nowDays - snapshotDays
	case "weekly":
		nowWeeks := int(now.Sub(firstMonday).Hours() / 24 / 7)
		snapshotWeeks := int(snapshotTime.Sub(firstMonday).Hours() / 24 / 7)
		i = nowWeeks - snapshotWeeks
	case "monthly":
		i = int(now.Month()) - int(snapshotTime.Month()) + 12*(now.Year()-snapshotTime.Year())
	default:
		err = fmt.Errorf("Invalid cleanup interval '%s'", interval)
	}
	return
}

func removeAllSymlinks(dir string) (err error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}
	for _, fi := range fileInfos {
		if fi.Mode()&os.ModeSymlink != 0 {
			err = os.Remove(path.Join(dir, fi.Name()))
			if err != nil {
				return
			}
		}
	}
	return
}

func clean(subvolume Subvolume, interval string, now time.Time, timestamps []Timestamp) (keptTimestamps TimestampMap, err error) {
	dir := path.Join(subvolume.Directory, subDir, interval)
	err = os.MkdirAll(dir, os.ModeDir|0700)
	if err != nil {
		return
	}
	err = removeAllSymlinks(dir)
	if err != nil {
		return
	}
	keptIndices := make(map[int]bool)
	keptTimestamps = make(TimestampMap)
	for _, timestamp := range timestamps {
		var i int
		i, err = calcIndex(now, timestamp.time, interval)
		if err != nil {
			fmt.Println(err.Error())
			err = nil
			continue
		}
		if _, ok := keptIndices[i]; ok {
			continue
		}
		keptIndices[i] = true
		keptTimestamps[timestamp.string] = true
		src := path.Join("..", "timestamp", timestamp.string)
		dst := path.Join(dir, strconv.Itoa(i))
		if *verboseFlag {
			fmt.Printf("Symlink '%s' => '%s'\n", dst, src)
		}
		err = os.Symlink(src, dst)
		if err != nil {
			return
		}
	}
	return
}

func (tm *TimestampMap) Merge(other TimestampMap) {
	for key, _ := range other {
		(*tm)[key] = true
	}
}

func cleanUp(subvolume Subvolume, nowTimestamp Timestamp) (err error) {
	timestampsDir := path.Join(subvolume.Directory, subDir, "timestamp")
	timestamps, err := readTimestampDir(timestampsDir)
	if err != nil {
		return
	}
	keptTimestamps := make(TimestampMap)
	keptTimestamps[nowTimestamp.string] = true
	var tempMap TimestampMap
	for _, interval := range Intervals {
		tempMap, err = clean(subvolume, interval, nowTimestamp.time, timestamps)
		if err != nil {
			return
		}
		keptTimestamps.Merge(tempMap)
	}
	// Remove unneeded timestamps
	for _, timestamp := range timestamps {
		if _, ok := keptTimestamps[timestamp.string]; !ok {
			var output []byte
			timestampLoc := path.Join(timestampsDir, timestamp.string)
			btrfsCmd := exec.Command(btrfsBin, "subvolume", "delete", timestampLoc)
			output, err = btrfsCmd.CombinedOutput()
			if err != nil {
				if !(*quietFlag) {
					fmt.Printf("%s", output)
				}
				return
			}
		}
	}
	return
}

func printCommand(cmd *exec.Cmd) {
	fmt.Printf("Running '%s %s'\n", cmd.Path, strings.Join(cmd.Args[1:], " "))
}

func receiveSnapshot(subvolume Subvolume, timestamp Timestamp) (err error) {
	targetPath := path.Join(subvolume.Directory, subDir, "timestamp")
	receiveCmd := exec.Command(btrfsBin, "receive", targetPath)
	receiveCmd.Stdin = os.Stdin
	receiveOut, err := receiveCmd.CombinedOutput()
	if err != nil {
		fmt.Print(receiveOut)
	}
	cleanUp(subvolume, timestamp)
	return
}

func sendSnapshot(snapshotPath string, remote SubvolumeRemote) (err error) {
	var sendErr bytes.Buffer
	sendCmd := exec.Command(btrfsBin, "send", snapshotPath)
	sendCmd.Stderr = &sendErr
	sendOut, err := sendCmd.StdoutPipe()
	if err != nil {
		return
	}

	var receiveOut bytes.Buffer
	var receiveCmd *exec.Cmd
	if remote.Host == "" {
		receiveCmd = exec.Command(btrfsBin, "receive", remote.Directory)
		receiveCmd.Stdin = sendOut
		receiveCmd.Stdout = &receiveOut
		receiveCmd.Stderr = &receiveOut
	} else {
		sshPath := remote.Host
		if remote.User != "" {
			sshPath = remote.User + "@" + sshPath
		}
		receiveArgs := []string{sshPath, "incrbtrfs", "-receive", remote.Directory, "-timestamp", path.Base(snapshotPath)}
		fmt.Println(remote.Limits.String())
		if remote.Limits.Hourly > 0 {
			receiveArgs = append(receiveArgs, "-hourly", strconv.Itoa(remote.Limits.Hourly))
		}
		if remote.Limits.Daily > 0 {
			receiveArgs = append(receiveArgs, "-daily", strconv.Itoa(remote.Limits.Daily))
		}
		if remote.Limits.Weekly > 0 {
			receiveArgs = append(receiveArgs, "-weekly", strconv.Itoa(remote.Limits.Weekly))
		}
		if remote.Limits.Monthly > 0 {
			receiveArgs = append(receiveArgs, "-monthly", strconv.Itoa(remote.Limits.Monthly))
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
		fmt.Println("Error with btrfs receive")
		fmt.Print(receiveOut.String())
		return
	}
	err = sendCmd.Run()
	if err != nil {
		fmt.Println("Error with btrfs send")
		fmt.Print(sendErr.String())
		return
	}
	err = receiveCmd.Wait()
	if err != nil {
		fmt.Println("Error with btrfs receive")
		fmt.Print(receiveOut.String())
		return
	}
	return
}

func runSnapshot(subvolume Subvolume, timestamp Timestamp) (err error) {
	targetPath := path.Join(subvolume.Directory, subDir, "timestamp", timestamp.string)
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "snapshot", "-r", subvolume.Directory, targetPath)
	if *verboseFlag {
		printCommand(btrfsCmd)
	}
	output, err := btrfsCmd.CombinedOutput()
	if err != nil {
		if !(*quietFlag) {
			fmt.Printf("%s", output)
		}
		return
	}
	if *verboseFlag {
		fmt.Printf("%s", output)
	}
	err = cleanUp(subvolume, timestamp)
	if err != nil {
		return
	}

	for _, remote := range subvolume.Remotes {
		err = sendSnapshot(targetPath, remote)
		if err != nil {
			fmt.Println("Error sending snapshot")
			fmt.Println(err.Error())
			err = nil
			continue
		}
	}
	return
}

func getCurrentTimestamp() Timestamp {
	currentTime := time.Now()
	s := currentTime.Format(timeFormat)
	return Timestamp{string: s, time: currentTime}
}

func runRemote() {
	if *timestampFlag == "" {
		fmt.Println("Must specify timestamp in receive mode")
		os.Exit(1)
	}
	var subvolume Subvolume
	subvolume.Directory = *receiveFlag
	subvolume.Limits = Limits{
		Hourly:  *hourlyFlag,
		Daily:   *dailyFlag,
		Weekly:  *weeklyFlag,
		Monthly: *monthlyFlag}
	timestamp, err := timestampFromString(*timestampFlag)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	err = receiveSnapshot(subvolume, timestamp)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func runLocal() {
	if flag.NArg() != 1 {
		fmt.Println("One Argument Required")
		os.Exit(1)
	}
	currentTimestamp := getCurrentTimestamp()
	config, err := parseFile(flag.Arg(0))
	if err != nil {
		fmt.Println("Erroring parsing file")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	subvolumes := parseConfig(config)
	isErr := false
	for _, subvolume := range subvolumes {
		if !(*quietFlag) {
			subvolume.Print()
		}
		err = runSnapshot(subvolume, currentTimestamp)
		if err != nil {
			fmt.Println(err)
			isErr = true
		}
	}
	if isErr {
		os.Exit(1)
	}

}

func main() {
	flag.Parse()
	if *receiveFlag != "" {
		runRemote()
	} else {
		runLocal()
	}
}
