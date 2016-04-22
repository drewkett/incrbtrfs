This is a btrfs snapshotting tool which is able to backup to another volume/machine using btrfs send/receive. It is intended to be run using a cron job with a simple TOML configuration file with the following format

```TOML
[defaults.limits]
hourly = 9
daily = 7
weekly = 4
monthly = 6

# /data backup
[[snapshot]]
directory = "/data"

[[snapshot.remote]]
directory = "/mnt/usb/data-backup"
[snapshot.remote.limits]
hourly = 3

[[snapshot.remote]]
directory = "/backups/host1/data"
host = "10.0.0.1"
port = "2222"
user = "backup"
exec = "/usr/local/bin/incrbtrfs"
[snapshot.remote.limits]
monthly = 0

# /home backup
[[snapshot]]
directory = /home
[snapshot.limits]
hourly = 0
```

- `[[snapshot]]` indicates a new snapshot specification
- `directory` specifies the subvolume to take a snapshot of
- `destination` specifies the directory that the snapshots are stored in. `$directory/.incrbtrfs` is the default
- `[[snapshot.remote]]` specifies that the snapshot should be sent somewhere. `directory` specifies the location of the backup. Remote snapshot locations do not append the .incrbtrfs folder.
  - `host`/`user`/`port` can be used to specify another machine to send the backups to. Communication is done with SSH. A copy of the incrbtrfs binary is required on the remote machine in order for this to work
  - `exec` can be used to specify the location of the `incrbtrfs` binary on the remote machine
- `[snapshot.limits]` specifies how many snapshots to maintain for each time frame. These inherhit any limits specified in [defaults.limits].
- `[snapshot.remote.limits]` specifies alternate settings for how many snapshots to keep at the remote destination

Note: The first time a snapshot is run with a remote specified, all of the data in the snapshot must be sent to the other drive, so it may take awhile. Future runs will reuse the existing snapshots as to only send the incrementally changed data.

The program can be installed with go install
```sh
go install github.com/drewkett/incrbtrfs
```

The program can be run by passing the config file as an argument

```sh
incrbtrfs sample.cfg
```

###Limitations
- If the btrfs receive command fails with message `ERROR: could not find parent subvolume`, there is currently no way to recover without manually deleting folder on the receive side that is supposedly a parent, but isn't. This is usually from a previously failed send/receive.
