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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"
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

type Backups []Backup

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
	newBackup.Created, err = time.Parse(BackupTimeFormat, backupTimeRaw)
	if err != nil {
		return err
	}
	*b = append(*b, newBackup)
	return nil
}

func (b *Backups) String() (backups string) {
	buf := new(bytes.Buffer)
	w := tabwriter.NewWriter(buf, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "Backups")
	fmt.Fprintln(w, "Name\tSize")
	for _, backup := range *b {
		fmt.Fprintln(w, backup.Name+"\t"+humanize.Bytes(uint64(backup.Size)))
	}
	w.Flush()
	return buf.String()
}

func (b *Backups) GetBackupsInDir(backupDir string) (backups []string) {
	files, _ := ioutil.ReadDir(backupDir)
	for _, f := range files {
		err := b.Add(backupDir + "/" + f.Name())
		if err != nil {
			log.Warn(err)
		}
	}
	return backups
}

// Backups implements sort.Interface for []Person based on Backup.Created
func (b *Backups) Len() int           { return len(*b) }
func (b *Backups) Swap(i, j int)      { (*b)[i], (*b)[j] = (*b)[j], (*b)[i] }
func (b *Backups) Less(i, j int) bool { return (*b)[i].Created.Before((*b)[j].Created) }

// SeparateBackupsByAge separates the backups by age
// The newest "countNew" backups are put in newBackups
// The older backups are put in oldBackups
func (b *Backups) SeparateBackupsByAge(countNew uint) (newBackups Backups, oldBackups Backups) {
	// Sort, the newest first
	sort.Sort(sort.Reverse(b))

	// If there are not enough backups, return all
	if (*b).Len() <= int(countNew) {
		return *b, nil
	}

	// Putt the newest in newBackups
	newBackups = (*b)[:countNew]
	oldBackups = (*b)[countNew:]

	if newBackups.Len() <= 0 && oldBackups.Len() > 0 {
		panic("No new backups, only old. Not sane! ")
	}
	return newBackups, oldBackups
}

func (b *Backups) DeleteAll() (count int, err error) {

	for _, backup := range *b {
		err = os.Remove(backup.Path)
		if err != nil {
			log.Warn(err)
		} else {
			count++
		}
	}
	return count, err
}
