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

The limits specify how many snapshots to maintain for each time frame. The remote directory specifies where to back up the snapshots too. Right now it only works for locally mounted drives, though ssh support is planned. 

The program can be run by passing the 

```sh
incrbtrfs sample.cfg
```
or
```
#This uses ~/.incrbtrfs.cfg as the config file
incrbtrfs
```

