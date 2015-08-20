package main

import (
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

const btrfsBin string = "/sbin/btrfs"
const subDir string = ".incrbtrfs"
const timeFormat string = "20060102_150405"

var Intervals = [...]string{"hourly", "daily", "weekly", "monthly"}
var quiet = flag.Bool("quiet", false, "Quiet Mode")
var verbose = flag.Bool("verbose", false, "Verbose Mode")

type Limits struct {
	Hourly  int
	Daily   int
	Weekly  int
	Monthly int
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
	fmt.Printf("Snapshot Dir='%s' (Hourly=%d, Daily=%d, Weekly=%d, Monthly=%d)\n",
		subvolume.Directory,
		subvolume.Limits.Hourly,
		subvolume.Limits.Daily,
		subvolume.Limits.Weekly,
		subvolume.Limits.Monthly)
	for _, remote := range subvolume.Remotes {
		dst := remote.Directory
		if remote.Host != "" {
			dst = strings.Join([]string{remote.Host, dst}, ":")
			if remote.User != "" {
				dst = strings.Join([]string{remote.User, dst}, "@")
			}
		}
		fmt.Printf("Remote Dir='%s' (Hourly=%d, Daily=%d, Weekly=%d, Monthly=%d)\n",
			dst,
			remote.Limits.Hourly,
			remote.Limits.Daily,
			remote.Limits.Weekly,
			remote.Limits.Monthly)
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

func readTimestampDir(timestampDir string) (timestamps []Timestamp, err error) {
	fileInfos, err := ioutil.ReadDir(timestampDir)
	if err != nil {
		return
	}
	for _, fi := range fileInfos {
		if fi.IsDir() {
			t, err := time.ParseInLocation(timeFormat, fi.Name(), time.Local)
			if err != nil {
				continue
			}
			timestamps = append(timestamps, Timestamp{string: fi.Name(), time: t})
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
		if *verbose {
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
				if !(*quiet) {
					fmt.Printf("%s", output)
				}
				return
			}
		}
	}
	return
}

func runSnapshot(subvolume Subvolume, timestamp Timestamp) (err error) {
	targetPath := path.Join(subvolume.Directory, subDir, "timestamp", timestamp.string)
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "snapshot", "-r", subvolume.Directory, targetPath)
	if *verbose {
		fmt.Printf("Running '%s %s'\n", btrfsCmd.Path, strings.Join(btrfsCmd.Args, " "))
	}
	output, err := btrfsCmd.CombinedOutput()
	if err != nil {
		if !(*quiet) {
			fmt.Printf("%s", output)
		}
		return
	}
	if *verbose {
		fmt.Printf("%s", output)
	}
	err = cleanUp(subvolume, timestamp)
	// TODO Handle Remotes w/ and w/o SSH
	return
}

func getCurrentTimestamp() Timestamp {
	currentTime := time.Now()
	s := currentTime.Format(timeFormat)
	return Timestamp{string: s, time: currentTime}
}

func main() {
	flag.Parse()
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
		if !(*quiet) {
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
