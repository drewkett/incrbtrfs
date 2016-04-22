package main

import (
	"github.com/BurntSushi/toml"
	"path"
)

type OptionalLimits struct {
	Hourly  *int
	Daily   *int
	Weekly  *int
	Monthly *int
}

type Config struct {
	Defaults struct {
		Limits OptionalLimits
		Remote struct {
			Limits OptionalLimits
		}
	}
	Snapshot []struct {
		Directory   string
		Destination string
		Limits      OptionalLimits
		Remote      []struct {
			Host      string
			Port      string
			User      string
			Exec      string
			Directory string
			Limits    OptionalLimits
		}
	}
}

func parseFile(configFile string) (config Config, err error) {
	_, err = toml.DecodeFile(configFile, &config)
	return
}

func parseConfig(config Config) (subvolumes []Subvolume) {
	var localDefaults Limits
	localDefaults = localDefaults.Merge(config.Defaults.Limits)
	remoteDefaults := localDefaults.Merge(config.Defaults.Remote.Limits)
	for _, snapshot := range config.Snapshot {
		var subvolume Subvolume
		var destination string
		subvolume.Directory = snapshot.Directory
		if snapshot.Destination == "" {
			destination = path.Join(subvolume.Directory, subDir)
		} else {
			destination = snapshot.Destination
		}
		subvolume.SnapshotsLoc = SnapshotsLoc{
			Directory: destination,
			Limits:    localDefaults.Merge(snapshot.Limits)}
		for _, remote := range snapshot.Remote {
			var remoteSnapshotsLoc RemoteSnapshotsLoc
			remoteSnapshotsLoc.User = remote.User
			remoteSnapshotsLoc.Host = remote.Host
			remoteSnapshotsLoc.Port = remote.Port
			if remoteSnapshotsLoc.Port == "" {
				remoteSnapshotsLoc.Port = "22"
			}
			remoteSnapshotsLoc.Exec = remote.Exec
			if remoteSnapshotsLoc.Exec == "" {
				remoteSnapshotsLoc.Exec = "incrbtrfs"
			}
			remoteSnapshotsLoc.SnapshotsLoc = SnapshotsLoc{
				Directory: remote.Directory,
				Limits:    remoteDefaults.Merge(snapshot.Limits, remote.Limits)}
			subvolume.Remotes = append(subvolume.Remotes, remoteSnapshotsLoc)
		}
		subvolumes = append(subvolumes, subvolume)
	}
	return subvolumes
}
