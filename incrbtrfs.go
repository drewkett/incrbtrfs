package main

import (
	"fmt"
	"os/exec"
	"os/user"
	"path"
	"strings"

	"github.com/BurntSushi/toml"
)

const btrfsBin string = "/sbin/btrfs"
const subDir string = ".incrbtrfs"

type OptionalLimits struct {
	Hourly  *int
	Daily   *int
	Weekly  *int
	Monthly *int
}

type Limits struct {
	Hourly  int
	Daily   int
	Weekly  int
	Monthly int
}

type Config struct {
	Defaults struct {
		Limits OptionalLimits
		Remote struct {
			Limits OptionalLimits
		}
	}
	// Snapshot []SnapshotConfig
	Snapshot []struct {
		Directory string
		Limits    OptionalLimits
		Remote    []struct {
			Host      string
			User      string
			Directory string
			Limits    OptionalLimits
		}
	}
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

func parseFile(configFile string) (config Config, err error) {
	_, err = toml.DecodeFile(configFile, &config)
	return
}

func parseConfig(config Config) (subvolumes []Subvolume) {
	var localDefaults Limits
	localDefaults = combineLimits(localDefaults, config.Defaults.Limits)
	remoteDefaults := combineLimits(localDefaults, config.Defaults.Remote.Limits)
	for _, snapshot := range config.Snapshot {
		var subvolume Subvolume
		subvolume.Directory = snapshot.Directory
		subvolume.Limits = combineLimits(localDefaults, snapshot.Limits)
		for _, remote := range snapshot.Remote {
			var subvolumeRemote SubvolumeRemote
			subvolumeRemote.User = remote.User
			subvolumeRemote.Host = remote.Host
			subvolumeRemote.Directory = remote.Directory
			subvolumeRemote.Limits = combineLimits(remoteDefaults, snapshot.Limits, remote.Limits)
			subvolume.Remotes = append(subvolume.Remotes, subvolumeRemote)
		}
		subvolumes = append(subvolumes, subvolume)
	}
	return subvolumes
}

func cleanUpOne(dir string, n int) (err error) {
	//TODO Clear directory. Iterate to find matches
	return
}

func cleanUp(subvolume Subvolume) (err error) {
	//TODO Read Timestamp directory and return kept map with all falses
	hourlyPath := path.Join(subvolume.Directory, subDir, "hourly")
	cleanUpOne(hourlyPath, subvolume.Limits.Hourly)
	dailyPath := path.Join(subvolume.Directory, subDir, "daily")
	cleanUpOne(dailyPath, subvolume.Limits.Daily)
	weeklyPath := path.Join(subvolume.Directory, subDir, "weekly")
	cleanUpOne(weeklyPath, subvolume.Limits.Weekly)
	monthlyPath := path.Join(subvolume.Directory, subDir, "monthly")
	cleanUpOne(monthlyPath, subvolume.Limits.Monthly)
	return
}

func runSnapshot(subvolume Subvolume) (err error) {
	targetPath := path.Join(subvolume.Directory, subDir, "timestamp")
	btrfsCmd := exec.Command(btrfsBin, "subvolume", "snapshot", "-r", subvolume.Directory, targetPath)
	output, err := btrfsCmd.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		fmt.Println(err)
		return
	}
	cleanUp(subvolume)
	return
}

func main() {
	//TODO Generate timestamp
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	configFile := path.Join(currentUser.HomeDir, ".incrbtrfs.cfg")
	config, err := parseFile(configFile)
	if err != nil {
		panic(err)
	}
	subvolumes := parseConfig(config)
	for _, subvolume := range subvolumes {
		subvolume.Print()
	}
	for _, subvolume := range subvolumes {
		runSnapshot(subvolume)
	}
}
