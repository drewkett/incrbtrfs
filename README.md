In process of converting to go. Old file can be found at incrbtrfs.py

This is a btrfs snapshotting tool which is able to backup to another volume using btrfs send/receive. It is intended to be run using a cron job with a simple TOML configuration file  with the following format 

```TOML
[default.limits]
hourly = 9
daily = 7
weekly = 4
monthly = 6

[[snapshot]]
directory = /data
[snapshot.remote]
directory = /mnt/usb
[snapshot.remote.limits]
hourly = 3

[[snapshot]]
directory = /home
[snapshot.limits]
hourly = 0
```

- `[[snapshot]]` indicates a new snapshot specification
- `directory` specifies the subvolume to take a snapshot of. Snapshots are stored in `$directory/.incrbtrfs`
- `[snapshot.remote]` specifies that the snapshot should be backed up somewhere. `directory` specifies the location of the backup. Currently it only works for locally mounted drives, though ssh support is planned. 
- `[snapshot.limits]` specifies how many snapshots to maintain for each time frame. These inherhit any limits specified in [default.limits].
- `[snapshot.remote.limits]` specifies alternate settings for how many snapshots to keep at the remote destination

Note: The first time a snapshot is run with a remote specified, all of the data in the snapshot must be sent to the other drive, so it may take awhile. Future runs will reuse the existing snapshots as to only send the incrementally changed data.

The program can be run by passing the config file as an argument

```sh
incrbtrfs sample.cfg
```
or by running it without arguments
```
#This uses ~/.incrbtrfs.cfg as the config file
incrbtrfs
```

