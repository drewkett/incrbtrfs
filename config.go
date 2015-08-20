package main

import "github.com/BurntSushi/toml"

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
