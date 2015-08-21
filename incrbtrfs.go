package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sort"
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

var Intervals = [...]string{"hourly", "daily", "weekly", "monthly"}
var quietFlag = flag.Bool("quiet", false, "Quiet Mode")
var verboseFlag = flag.Bool("verbose", false, "Verbose Mode")
var receiveCheckFlag = flag.String("receiveCheck", "", "Receive Mode (Check)")
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
	Directory         string
	SnapshotDirectory string
	Limits            Limits
	Remotes           []SubvolumeRemote
}

// type Timestamp struct {
// 	string string
// 	time   time.Time
// }
type Timestamp string
type Timestamps []Timestamp
type TimestampMap map[Timestamp]bool

func (p Timestamps) Len() int           { return len(p) }
func (p Timestamps) Less(i, j int) bool { return string(p[i]) < string(p[j]) }
func (p Timestamps) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

func parseTimestamp(timestamp Timestamp) (t time.Time, err error) {
	t, err = time.ParseInLocation(timeFormat, string(timestamp), time.Local)
	return
}

func readTimestampsDir(snapshotsDir string) (timestamps []Timestamp, err error) {
	timestampsDir := path.Join(snapshotsDir, "timestamp")
	os.MkdirAll(timestampsDir, 0700|os.ModeDir)
	fileInfos, err := ioutil.ReadDir(timestampsDir)
	if err != nil {
		return
	}
	for _, fi := range fileInfos {
		if fi.IsDir() {
			timestamp := Timestamp(fi.Name())
			_, err := parseTimestamp(timestamp)
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
		var snapshotTime time.Time
		snapshotTime, err = parseTimestamp(timestamp)
		if err != nil {
			continue
		}
		i, err = calcIndex(now, snapshotTime, interval)
		if err != nil {
			fmt.Println(err.Error())
			err = nil
			continue
		}
		if _, ok := keptIndices[i]; ok {
			continue
		}
		keptIndices[i] = true
		keptTimestamps[timestamp] = true
		src := path.Join("..", "timestamp", string(timestamp))
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

func cleanUp(subvolume Subvolume, nowTimestamp Timestamp, timestamps []Timestamp) (err error) {
	now, err := parseTimestamp(nowTimestamp)
	if err != nil {
		return
	}
	timestampsDir := path.Join(subvolume.Directory, subDir, "timestamp")
	keptTimestamps := make(TimestampMap)
	keptTimestamps[nowTimestamp] = true
	var tempMap TimestampMap
	for _, interval := range Intervals {
		tempMap, err = clean(subvolume, interval, now, timestamps)
		if err != nil {
			return
		}
		keptTimestamps.Merge(tempMap)
	}
	// Remove unneeded timestamps
	for _, timestamp := range timestamps {
		if _, ok := keptTimestamps[timestamp]; !ok {
			var output []byte
			timestampLoc := path.Join(timestampsDir, string(timestamp))
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

func receiveSnapshot(subvolume Subvolume, timestamp Timestamp) (err error) {
	targetPath := path.Join(subvolume.SnapshotDirectory, "timestamp")
	receiveCmd := exec.Command(btrfsBin, "receive", targetPath)
	receiveCmd.Stdin = os.Stdin
	receiveOut, err := receiveCmd.CombinedOutput()
	if err != nil {
		fmt.Print(receiveOut)
	}
	timestamps, err := readTimestampsDir(subvolume.SnapshotDirectory)
	if err != nil {
		return
	}
	err = cleanUp(subvolume, timestamp, timestamps)
	if err != nil {
		return
	}
	return
}

func getRemoteTimestamps(remote SubvolumeRemote) (timestamps []Timestamp, err error) {
	sshPath := remote.Host
	if remote.User != "" {
		sshPath = remote.User + "@" + sshPath
	}
	var receiveCheckOut []byte
	var receiveCheckErr bytes.Buffer
	receiveCheckCmd := exec.Command("ssh", sshPath, "incrbtrfs", "-receiveCheck", remote.Directory)
	receiveCheckCmd.Stderr = &receiveCheckErr
	receiveCheckOut, err = receiveCheckCmd.Output()
	if err != nil {
		fmt.Println("Failed to run ReceiveCheck")
		fmt.Println(string(receiveCheckOut))
		fmt.Println(receiveCheckErr.String())
		return
	}
	fmt.Println(string(receiveCheckOut))
	var checkStr RemoteCheck
	err = json.Unmarshal(receiveCheckOut, &checkStr)
	if err != nil {
		fmt.Println("Failed to read ReceiveCheck JSON")
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

func sendSnapshot(snapshotPath string, remote SubvolumeRemote, parent Timestamp) (err error) {
	var sendErr bytes.Buffer
	var sendCmd *exec.Cmd
	if parent == "" {
		if *verboseFlag {
			fmt.Println("Performing full send/receive")
		}
		sendCmd = exec.Command(btrfsBin, "send", snapshotPath)
	} else {
		if *verboseFlag {
			fmt.Println("Performing incremental send/receive")
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
		remoteTarget := path.Join(remote.Directory, "timestamp")
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
		// 	fmt.Println("Failed to run ReceiveCheck")
		// 	fmt.Println(string(receiveCheckOut))
		// 	fmt.Println(receiveCheckErr.String())
		// 	return
		// }
		// var checkStr RemoteCheck
		// err = json.Unmarshal(receiveCheckOut, &checkStr)
		// if err != nil {
		// 	fmt.Println("Failed to read ReceiveCheck JSON")
		// 	return
		// }
		// if checkStr.Version != version {
		// 	err = fmt.Errorf("Incompatible Version Local (%d) != Remote (%d)", version, checkStr.Version)
		// 	return
		// }
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
	targetPath := path.Join(subvolume.Directory, subDir, "timestamp", string(timestamp))
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

	timestamps, err := readTimestampsDir(subvolume.SnapshotDirectory)
	if err != nil {
		return
	}
	err = cleanUp(subvolume, timestamp, timestamps)
	if err != nil {
		return
	}

	for _, remote := range subvolume.Remotes {
		var remoteTimestamps []Timestamp
		if remote.Host == "" {
			remoteTimestamps, err = readTimestampsDir(remote.Directory)
			if err != nil {
				fmt.Println(err.Error())
				err = nil
				continue
			}
		} else {
			remoteTimestamps, err = getRemoteTimestamps(remote)
			if err != nil {
				fmt.Println(err.Error())
				err = nil
				continue
			}
		}
		parentTimestamp := calcParent(timestamps, remoteTimestamps)
		if *verboseFlag && parentTimestamp != "" {
			fmt.Printf("Parent = %s\n", string(parentTimestamp))
		}
		err = sendSnapshot(targetPath, remote, parentTimestamp)
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
		fmt.Print(err.Error())
		os.Exit(1)
	}
	fis, err := ioutil.ReadDir(timestampsDir)
	if err != nil {
		fmt.Print(err.Error())
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
		fmt.Print(err)
		os.Exit(1)
	}
	n, err := os.Stdout.Write(data)
	if err != nil || n != len(data) {
		fmt.Print(err)
		os.Exit(1)
	}
}

func runRemote() {
	if *timestampFlag == "" {
		fmt.Println("Must specify timestamp in receive mode")
		os.Exit(1)
	}
	var subvolume Subvolume
	subvolume.Directory = *receiveFlag
	subvolume.SnapshotDirectory = *receiveFlag
	subvolume.Limits = Limits{
		Hourly:  *hourlyFlag,
		Daily:   *dailyFlag,
		Weekly:  *weeklyFlag,
		Monthly: *monthlyFlag}
	timestamp := Timestamp(*timestampFlag)
	_, err := parseTimestamp(timestamp)
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
	if *receiveCheckFlag != "" {
		runRemoteCheck()
	} else if *receiveFlag != "" {
		runRemote()
	} else {
		runLocal()
	}
}
