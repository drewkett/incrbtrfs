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
user = "backup"
[snapshot.remote.limits]
monthly = 0

# /home backup
[[snapshot]]
directory = /home
[snapshot.limits]
hourly = 0
```

- `[[snapshot]]` indicates a new snapshot specification
- `directory` specifies the subvolume to take a snapshot of. Snapshots are stored in `$directory/.incrbtrfs`
- `[[snapshot.remote]]` specifies that the snapshot should be sent somewhere. `directory` specifies the location of the backup. Remote snapshot locations do not append the .incrbtrfs folder.
  - `host`/`user` can be used to specify another machine to send the backups to. Communication is done with SSH. A copy of the incrbtrfs binary is required on the remote machine in order for this to work
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
