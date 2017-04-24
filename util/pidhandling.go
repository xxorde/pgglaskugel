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

	log "github.com/Sirupsen/logrus"
)

// WritePidFile checks for existent files and writes the pidfile.
func WritePidFile(pidfile string) error {
	if pidfile == "" {
		return errors.New("pidfile is not configured/empty")
	}

	// if the paths to the pdfile doesn't exist, we create them
	if err := os.MkdirAll(filepath.Dir(pidfile), os.FileMode(0755)); err != nil {
		return err
	}

	file, err := os.Create(pidfile)
	if err != nil {
		log.Errorf("error opening pidfile %s: %s", pidfile, err)
		return fmt.Errorf("error opening pidfile %s: %s", pidfile, err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d", os.Getpid())
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

// ReadPid from the configured file. It is an error if the pidfile hasn't
// been configured.
func ReadPid(pidfile string) (int, error) {
	pidcontent, err := ioutil.ReadFile(pidfile)
	if err != nil {
		return 0, err
	}

	pid, err := strconv.Atoi(string(bytes.TrimSpace(pidcontent)))
	if err != nil {
		log.Errorf("error parsing pid from %s: %s", pidfile, err)
		return 0, fmt.Errorf("error parsing pid from %s: %s", pidfile, err)
	}

	return pid, nil
}

// DeletePidFile deletes the pidfile and returns an error if something fails
func DeletePidFile(pidfile string) error {
	if err := os.Remove(pidfile); err != nil {
		log.Errorf("error deleting pidfile %s : %s", pidfile, err)
		return fmt.Errorf("error deleting pidfile %s : %s", pidfile, err)
	}
	return nil
}

//CheckPid gives an error if the PID does not exist on the system
// returns true if the actual pid is the same as the stored pid
func CheckPid(pidfile string) error {
	if _, err := os.Stat(pidfile); os.IsNotExist(err) {
		return nil
	}

	actualpid := os.Getpid()
	storedpid, err := ReadPid(pidfile)
	if err != nil {
		return err
	}
	// if the actual pid differs from the stored pid, look if its an old entry or still active via /proc
	if actualpid != storedpid {
		procstatfile := fmt.Sprintf("/proc/%d/stat", storedpid)
		if _, err := os.Stat(procstatfile); err == nil {
			return fmt.Errorf("Pid in pidfile(%d) differs from actual pid(%d) and is still active", storedpid, actualpid)
		}
		// There is an old pid in the file, but the process is not active anymore. We can delete it and go on
		DeletePidFile(pidfile)
	}
	return nil
}
