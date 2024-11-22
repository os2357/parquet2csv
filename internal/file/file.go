package file

import (
	"errors"
	"os"
	"syscall"
)

func IsExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, err
	} else if err != nil {
		panic(err)
	}
	return true, nil
}

func Info(path string) (os.FileInfo, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, err
	} else if err != nil {
		panic(err)
	}
	return info, nil
}

func IsWritable(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, errors.New("Path doesn't exist. " + path)
	}

	if !info.IsDir() {
		return false, errors.New("Path isn't a directory. " + path)
	}

	if info.Mode().Perm()&(1<<(uint(7))) == 0 { //nolint:mnd // num write bit
		return false, errors.New("Write permission bit is not set on this file for user. " + path)
	}

	var stat syscall.Stat_t
	if err = syscall.Stat(path, &stat); err != nil {
		return false, errors.New("Unable to get stat. " + path)
	}

	if uint32(os.Geteuid()) != stat.Uid {
		return false, errors.New("User doesn't have permission to write to this directory. " + path)
	}
	return true, nil
}
