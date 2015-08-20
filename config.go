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
