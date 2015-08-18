This is a btrfs snapshotting tool which is able to backup to another volume using btrfs send/receive. It is intended to be run using a cron job with a simple TOML configuration file  with the following format 

```TOML
[default.limits]
hourly = 9
daily = 7
weekly = 4
monthly = 6

[[snapshot]]
directory = /data
remote.directory = /mnt/usb
remote.limits.hourly = 3

[[snapshot]]
directory = /home
limits.hourly = 0
```

- Anything specified in `[default.limits]` applies to all of the backups
- `[[snapshot]]` indicates a new snapshot
- `directory` specifies the subvolume to take a snapshot of. Snapshots are stored in `$directory/.incrbtrfs`
- `remote.directory` specifies where to back up the snapshots too. Right now it only works for locally mounted drives, though ssh support is planned. 
- `limits.*` specifies how many snapshots to maintain for each time frame
` `remote.limits.*` specifies alternate settings for how many snapshots to keep at the remote destination

Note: The first time a snapshot is run with `remote.directory` specified, all of the data in the snapshot must be sent to the other drive, so it may take awhile. Future runs will reuse the existing snapshots as to only send the incrementally changed data.

The program can be run by passing the config file as an argument

```sh
incrbtrfs sample.cfg
```
or by running it without arguments
```
#This uses ~/.incrbtrfs.cfg as the config file
incrbtrfs
```

