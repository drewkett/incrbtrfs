package main

import (
	"fmt"
	"os"
	"syscall"
)

type DirLock struct {
	file *os.File
	path string
}

func NewDirLock(dir string) (lock DirLock, err error) {
	err = os.MkdirAll(dir, dirMode)
	if err != nil {
		return
	}
	file, err := os.Open(dir)
	if err != nil {
		err = fmt.Errorf("Failed to open directory '%s' for locking", dir)
		return
	}
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		err = fmt.Errorf("Failed to acquire lock for '%s'", dir)
		return
	}
	lock = DirLock{file, dir}
	return
}

func (lock DirLock) Unlock() (err error) {
	err = syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	if err != nil {
		err = fmt.Errorf("Failed to unlock '%s'", lock.path)
	}
	err = lock.file.Close()
	if err != nil {
		err = fmt.Errorf("Failed to close directory '%s' after unlock", lock.path)
	}
	return
}
