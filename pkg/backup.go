// Copyright © 2016 Alexander Sosna <alexander@xxor.de>
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
package pkg

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/siddontang/go/log"
)

const (
	BackupTimeFormat = "2006-01-02T15:04:05"
)

type Backup struct {
	Name      string
	Extension string
	Path      string
	Size      int64
	Created   time.Time
	Sane      bool
}

type Backups struct {
	Backup []Backup
}

func (b *Backups) Add(path string) (err error) {
	var newBackup Backup
	// Make a relative path absolute
	newBackup.Path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	newBackup.Extension = filepath.Ext(path)
	// Get the name of the backup file without the extension
	newBackup.Name = strings.TrimSuffix(filepath.Base(path), newBackup.Extension)

	// Get size of backup
	file, err := os.Open(path)
	defer file.Close()

	if err != nil {
		return err
	}
	fi, err := file.Stat()
	if err != nil {
		return err
	}
	newBackup.Size = fi.Size()

	// Remove anything before the '@'
	reg := regexp.MustCompile(`.*@`)
	backupTimeRaw := reg.ReplaceAllString(newBackup.Name, "${1}")
	log.Debug("Parse as time: ", backupTimeRaw)
	newBackup.Created, err = time.Parse(BackupTimeFormat, backupTimeRaw)
	if err != nil {
		return err
	}

	log.Warn(newBackup.Name + "   " + newBackup.Created.String())
	b.Backup = append(b.Backup, newBackup)
	return nil
}
