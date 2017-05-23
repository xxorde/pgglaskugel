// package util - pidhandling module
// Copyright © 2017 Alexander Sosna <alexander@xxor.de>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package util

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func closepidfile(pidfile *os.File) {
	syscall.Flock(int(pidfile.Fd()), syscall.LOCK_UN)
	pidfile.Close()
}

// WritePidFile checks for existent files and writes the pidfile if the check is succesful
func WritePidFile(pidfile string) error {
	if pidfile == "" {
		return errors.New("pidfile is not configured/empty")
	}

	// if the paths to the pdfile doesn't exist, we create them
	if err := os.MkdirAll(filepath.Dir(pidfile), os.FileMode(0755)); err != nil {
		return err
	}

	// check the pidfile and get the filehandle if everything is fine
	file, err := checkpid(pidfile)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(file, "%d", os.Getpid())
	if err != nil {
		return err
	}
	closepidfile(file)
	return nil
}

// DeletePidFile deletes the pidfile and returns an error if something fails
func DeletePidFile(pidfile string) error {
	if err := os.Remove(pidfile); err != nil {
		return fmt.Errorf("error deleting pidfile %s : %s", pidfile, err)
	}
	return nil
}

func createpidfile(pidfile string) (*os.File, error) {
	file, err := os.Create(pidfile)
	if err != nil {
		return nil, fmt.Errorf("error opening pidfile %s: %s", pidfile, err)
	}
	syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
	return file, nil
}

// format the output from /proc/pid/cmdline into a readable string
func formatcmdline(cmdcontent []byte) string {
	cfslice := bytes.Split(cmdcontent, []byte{0})
	var bytostr []string
	for _, p := range cfslice {
		bytostr = append(bytostr, string(p))
	}
	cmdstring := strings.Join(bytostr, " ")
	return cmdstring
}

// checkpid gives an error if the PID does not exist on the system
// returns true if the actual pid is the same as the stored pid
func checkpid(pidfile string) (*os.File, error) {

	// if the file doesn't exist, create the file, lock it, and return the filehandle
	// so writepid kann write it
	if _, err := os.Stat(pidfile); os.IsNotExist(err) {
		return createpidfile(pidfile)
	}

	// if the file exists, compare the stored pid with the actual and give a reply
	actualpid := os.Getpid()
	file, err := os.Open(pidfile)
	syscall.Flock(int(file.Fd()), syscall.LOCK_EX)

	pidcontent, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	storedpid, err := strconv.Atoi(string(bytes.TrimSpace(pidcontent)))
	if err != nil {
		DeletePidFile(pidfile)
		return createpidfile(pidfile)

	}

	// if the actual pid differs from the stored pid, look if its an old entry or still active via /proc
	if actualpid != storedpid {
		procfile := fmt.Sprintf("/proc/%d/cmdline", storedpid)
		if _, err := os.Stat(procfile); err == nil {
			pf, err := os.Open(procfile)
			if err != nil {
				return nil, fmt.Errorf("Error opening /proc/%d/cmdline: %s", storedpid, err)
			}
			defer pf.Close()
			cfoutput, _ := ioutil.ReadAll(pf)
			command := formatcmdline(cfoutput)
			return nil, fmt.Errorf("Pid in pidfile(%s : %d) differs from actual pid(%d) and is still active. /proc/cmdline: %s", pidfile, storedpid, actualpid, command)
		}
		// There is an old pid in the file, but the process is not active anymore. We can delete it and go on
		DeletePidFile(pidfile)
		return createpidfile(pidfile)
	}
	return file, nil
}
