#!/usr/bin/env python

from datetime import datetime,timedelta
now = datetime.now()
monday = datetime(year=1970,month=1,day=5)

import toml
import socket
import signal
from sys import stderr
from argparse import ArgumentParser
from subprocess import check_call,PIPE,Popen,CalledProcessError
from os.path import join,exists,basename,lexists,realpath,dirname,expanduser,isdir
from os import makedirs, listdir, symlink, remove
from collections import namedtuple

dry_run = False
def safe_symlink(src,dst):
    if dry_run:
        print("Link {dst} -> {src}".format(src=src,dst=dst))
    else:
        symlink(src,dst)

def safe_remove(loc):
    if dry_run:
        print("Remove {loc}".format(loc=loc))
    else:
        remove(loc)

def safe_makedirs(loc):
    if dry_run:
        print("Make Dirs {loc}".format(loc=loc))
    else:
        makedirs(loc)

def safe_call(cmd):
    if dry_run:
        print("Call '{cmd}'".format(cmd=" ".join(cmd)))
    else:
        check_call(cmd)

timestamp_strp = "%Y%m%d_%H%M%S_%f"

parse_timestamp = lambda loc: datetime.strptime(basename(realpath(loc)),timestamp_strp)

signal.signal(signal.SIGINT,signal.SIG_IGN)

SnapLoc = namedtuple("SnapLoc",["directory","limits"])
SnapLimits = namedtuple("SnapLimits",["hourly","daily","weekly","monthly"])
class Snapshot(object):
    def __init__(self,subvolume,timestamp_str):
        self.subvolume = subvolume
        self.loc = join(self.subvolume,".incrbtrfs","timestamp",timestamp_str)
        self.timestamp_str = timestamp_str
        self.timestamp = datetime.strptime(timestamp_str,timestamp_strp)
        self.keep = False

    def __repr__(self):
        ret = self.loc
        if self.keep:
            ret += " KEEP"
        return ret

def round_time(timestamp):
    timestamp = timestamp.replace(minute=0,second=0,microsecond=0)
    return timestamp

def calc_index(snapshot_dir,snaptype):
    timestamp = round_time(parse_timestamp(snapshot_dir))
    now_rounded = round_time(now)
    if snaptype == "hourly":
        hr_now = (now-monday).seconds//3600 + (now-monday).days*24
        hr_timestamp = (timestamp-monday).seconds//3600 + (timestamp-monday).days*24
        return hr_now - hr_timestamp
    elif snaptype == "daily":
        return ((now-monday).days - (timestamp-monday).days)
    elif snaptype == "weekly":
        return (now-monday).days//7 - (timestamp-monday).days//7
    elif snaptype == "monthly":
        return now.month - timestamp.month + 12*(now.year - timestamp.year)

def mark_snapshots(snap_loc,snapshots,snaptype):
    limit = getattr(snap_loc.limits,snaptype)
    link_dir = join(snap_loc.directory,".incrbtrfs",snaptype)
    if not exists(link_dir):
        makedirs(link_dir)
    
    snapshots_base = [basename(snapshot.loc) for snapshot in snapshots]
    snapshots_i = [calc_index(snapshot.loc,snaptype) for snapshot in snapshots]
    for i in range(limit,-1,-1):
        index_dir = join(link_dir,str(i))
        if lexists(index_dir):
            safe_remove(index_dir)
        if i in snapshots_i:
            last_index = list(reversed(snapshots_i)).index(i)
            s = list(reversed(snapshots))[last_index]
            s.keep = True
            rel_loc = join("..","timestamp",s.timestamp_str)
            safe_symlink(rel_loc,index_dir)

def mark_last(snapshots):
    snap = max(snapshots, key=lambda s: s.timestamp)
    last_dir = join(snap.loc,"..","..","last")
    if lexists(last_dir):
        safe_remove(last_dir)
    rel_loc = join("timestamp",snap.timestamp_str)
    safe_symlink(rel_loc,last_dir)
    snap.keep = True
    return snap

def get_base_dir(subvolume):
    return join(subvolume,".incrbtrfs","timestamp")

def snapshot(subvolume):
    base = get_base_dir(subvolume)
    if not exists(base):
        makedirs(base)
    location = join(base,timestamp)
    command = ('/sbin/btrfs','subvolume','snapshot','-r',subvolume,location)
    try:
        safe_call(command)
        if dry_run:
            check_call(("/usr/bin/touch",location))
    except CalledProcessError:
        stderr.write("Failed to create snapshot\n")
        if exists(location):
            rm_cmd = ("/sbin/btrfs","subvolume","delete",location)
            try:
                safe_call(rm_cmd)
            except CalledProcessError:
                stderr.write("Couldn't delete failed subvolume")
        return None
    return Snapshot(subvolume,timestamp)

limit_names = ("hourly","daily","weekly","monthly")
def extract_limits(d):
    ret = {}
    for name in limit_names:
        if name in d:
            if not isinstance(d[name],int):
                stderr.write("Limits must be integers\n. Ignoring")
            ret[name] = d[name]
    return ret

def read_config(filename):
    subvolumes = []
    default_limits = SnapLimits(hourly=0,daily=0,weekly=0,monthly=0)

    config = toml.load(expanduser(filename))
    if "default" in config:
        if "limits" in config["default"]:
            default_limits = default_limits._replace(**extract_limits(config["default"]["limits"]))

    for section in config["snapshot"]:
        if 'directory' not in section:
            stderr.write("'directory' not specified for snapshot. Skipping\n")
            continue
        local_dir = section['directory']
        limits = default_limits
        if 'limits' in section:
            limits = limits._replace(**extract_limits(section['limits']))
        local_snap_loc = SnapLoc(local_dir,limits)
        remote_snap_loc = None
        if 'remote' in section:
            remote_section = section['remote']
            if 'directory' not in remote_section:
                stderr.write("'remote' specified without directory. Skipping\n")
                continue
            remote_dir = remote_section['directory']
            if 'limits' in remote_section:
                limits = limits._replace(**extract_limits(section['limits']))
            remote_snap_loc = SnapLoc(remote_dir,limits)
        yield(local_snap_loc,remote_snap_loc)

def clean_up(snap_loc):
    subvolume = snap_loc.directory
    base = get_base_dir(subvolume)
    if dry_run:
        snapshots = [Snapshot(subvolume,t) for t in listdir(base)]
    else:
        snapshots = [Snapshot(subvolume,t) for t in listdir(base) if isdir(join(base,t))]
    snapshots = sorted(snapshots,key=lambda s: s.timestamp,reverse=True)
    last = mark_last(snapshots)
    if snap_loc.limits.hourly:
        mark_snapshots(snap_loc,snapshots,"hourly")
    if snap_loc.limits.daily:
        mark_snapshots(snap_loc,snapshots,"daily")
    if snap_loc.limits.weekly:
        mark_snapshots(snap_loc,snapshots,"weekly")
    if snap_loc.limits.monthly:
        mark_snapshots(snap_loc,snapshots,"monthly")

    if not any(snapshot.keep for snapshot in snapshots):
        stderr.write("None marked\n")
        return

    for snapshot in snapshots:
        if snapshot.keep:
            continue
        command = ("/sbin/btrfs","subvolume","delete",snapshot.loc)
        safe_call(command)
    if dry_run:
        for snap in listdir(base):
            if not isdir(join(base,snap)):
                remove(join(base,snap))
    return last

def send_snapshot(local_snap,remote_snap_dir):
    local_snap_dir = local_snap.subvolume
    parent_timestamp = get_parent(local_snap_dir,remote_snap_dir)
    remote_base = get_base_dir(remote_snap_dir)
    remote_subvolume = join(remote_base,local_snap.timestamp_str)
    if parent_timestamp:
        parent_loc = join(local_snap_dir,".incrbtrfs","timestamp",parent_timestamp)
        stderr.write("Performing Incremental Send/Receive\n")
        stderr.write("Sending = %s\n"%snap.loc)
        stderr.write("Parent = %s\n"%parent_loc)
        snd_cmd = ("/sbin/btrfs","send","-p",parent_loc,snap.loc)
    else:
        stderr.write("Performing Full Send/Receive\n")
        stderr.write("Sending = %s\n"%snap.loc)
        snd_cmd = ("/sbin/btrfs","send",snap.loc)
    rcv_cmd = ("/sbin/btrfs","receive",remote_base)
    stderr.write("Receiving = %s\n"%remote_subvolume)
    if dry_run:
        print("Calling '{snd} | {rcv}'".format(snd=" ".join(snd_cmd),rcv=" ".join(rcv_cmd)))
        return True
    safe_call(("sync",))
    snd_p = Popen(snd_cmd,stdout=PIPE)
    rcv_p = Popen(rcv_cmd,stdin=snd_p.stdout)
    snd_p.stdout.close()
    rcv_p.communicate()
    snd_p.wait()
    if snd_p.returncode != 0 or rcv_p.returncode != 0:
        if snd_p.returncode != 0 and rcv_p.returncode != 0:
            stderr.write("Send/Receive failed\n")
        elif snd_p.returncode != 0:
            stderr.write("Send failed\n")
        else:
            stderr.write("Receive failed\n")
        if exists(remote_subvolume):
            rm_cmd = ("/sbin/btrfs","subvolume","delete",remote_subvolume)
            try:
                safe_call(rm_cmd)
            except CalledProcessError:
                stderr.write("Couldn't delete failed subvolume")
        return False
    return True

    
timestamp = now.strftime(timestamp_strp)

def get_parent(local_snap_loc,remote_snap_loc):
    local_base = get_base_dir(local_snap_loc)
    local_timestamps = listdir(local_base)
    remote_base = get_base_dir(remote_snap_loc)
    if not exists(remote_base):
        makedirs(remote_base)
    remote_timestamps = listdir(remote_base)
    timestamps = local_timestamps + remote_timestamps
    timestamps = sorted(timestamps,reverse=True)
    parent = None
    for t1,t2 in zip(timestamps[:-1],timestamps[1:]):
        if t1 == t2:
            parent = t1
            break
    return parent

def get_snap_limits(cfg):
    local_limits = []
    remote_limits = []
    local_limits = tuple(cfg["%s_limit"%k] for k in ("hourly","daily","weekly","monthly"))
    remote_limits = tuple(cfg["remote_%s_limit"%k] for k in ("hourly","daily","weekly","monthly"))
    local_snap_limits = SnapLimits(*local_limits)
    remote_snap_limits = SnapLimits(*remote_limits)
    return local_snap_limits, remote_snap_limits

def get_snap_locs(cfg):
    local_snap_limits, remote_snap_limits = get_snap_limits(cfg)
    local_snap_loc = SnapLoc(cfg["directory"],local_snap_limits)
    if "remote_directory" in cfg:
        remote_snap_loc = SnapLoc(cfg["remote_directory"],remote_snap_limits)
    else:
        remote_snap_loc = None
    return local_snap_loc, remote_snap_loc

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("cfg_file",default="~/.incrbtrfs.cfg",nargs='?')
    parser.add_argument("-n","--dry-run",help="Dry Run",default=False,action="store_true")
    args = parser.parse_args()

    dry_run = args.n

    lock = socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)
    try:
        lock.bind('\0incrbtrfs')
    except:
        stderr.write("Another incrbtrfs process is running\n")
        exit(1)
    for local_snap_loc, remote_snap_loc in read_config(args.cfg_file):
        try:
            snap = snapshot(local_snap_loc.directory)
        except CalledProcessError:
            continue
        if remote_snap_loc:
            success = send_snapshot(snap,remote_snap_loc.directory)
            if success:
                clean_up(remote_snap_loc)
        if snap:
            clean_up(local_snap_loc)