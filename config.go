package main

import (
	"path"

	"github.com/BurntSushi/toml"
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
		subvolume.Directory = snapshot.Directory
		subvolume.SnapshotLoc = SnapshotLoc{
			Directory: path.Join(subvolume.Directory, subDir),
			Limits:    localDefaults.Merge(snapshot.Limits)}
		for _, remote := range snapshot.Remote {
			var remoteSnapshotLoc RemoteSnapshotLoc
			remoteSnapshotLoc.User = remote.User
			remoteSnapshotLoc.Host = remote.Host
			remoteSnapshotLoc.SnapshotLoc = SnapshotLoc{
				Directory: remote.Directory,
				Limits:    remoteDefaults.Merge(snapshot.Limits, remote.Limits)}
			subvolume.Remotes = append(subvolume.Remotes, remoteSnapshotLoc)
		}
		subvolumes = append(subvolumes, subvolume)
	}
	return subvolumes
}
