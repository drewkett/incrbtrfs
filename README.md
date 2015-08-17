This is a btrfs snapshotting tool which is able to backup to another volume using btrfs send/receive. It runs with a simple configuration file (parsed by ConfigParser in the python std lib) with the following format 

```INI
[DEFAULT]
hourly_limit = 9
daily_limit = 7
weekly_limit = 4
monthly_limit = 6

[data]
directory = /data
remote_directory = /mnt/usb
remote_hourly_limit = 3

[home]
directory = /home
hourly_limit = 0
```

- Anything specified in `[DEFAULT]` applies to all of the backups
- `directory` specifies the subvolume to take a new snapshot of. Snapshots are stored in `$directory/.incrbtrfs`
- `remote directory` specifies where to back up the snapshots too. Right now it only works for locally mounted drives, though ssh support is planned. 
- `*_limit` specifies how many snapshots to maintain for each time frame
` `remote_*_limit` specifies alternate settings for how many snapshots to keep at the remote destination

The program can be run by passing the config file as an argument

```sh
incrbtrfs sample.cfg
```
or by running it without arguments
```
#This uses ~/.incrbtrfs.cfg as the config file
incrbtrfs
```

It is recommended to run the program using cron once every hour.
