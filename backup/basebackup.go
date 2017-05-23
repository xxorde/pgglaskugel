// Package backup - basebackup module
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
package backup

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/siddontang/go/log"
)

// IsSane returns true if the backup seams sane
func (b *Backup) IsSane() (sane bool) {
	if b.Size < SaneBackupMinSize {
		return false
	}
	return true
}

// AddBackupLabel checks the backup name
func (b *Backup) AddBackupLabel(backupLabel []byte) (err error) {

	// Parses the input name as backup label and returns an array
	// 0 contains full string
	// 1 contains WAL name
	startWal := findStartWalLine.FindStringSubmatch(string(backupLabel))
	if len(startWal) < 2 {
		log.Debug("startWal ", startWal)
		return errors.New("Can not find START WAL")
	}

	b.StartWalLocation = string(startWal[1])
	return nil
}

// IsSane returns false if at leased one backups seams not sane
func (b *Backups) IsSane() (sane bool) {
	for _, backup := range b.Backup {
		if backup.IsSane() != true {
			return false
		}
	}
	return true
}

// Sane returns all backups that seem sane
func (b *Backups) Sane() (sane Backups) {
	for _, backup := range b.Backup {
		if backup.IsSane() == true {
			sane.Backup = append(sane.Backup, backup)
		}
	}
	return sane
}

// Insane returns all backups that seem not sane
func (b *Backups) Insane() (insane Backups) {
	for _, backup := range b.Backup {
		if backup.IsSane() != true {
			insane.Backup = append(insane.Backup, backup)
		}
	}
	return insane
}

// Add adds an WAL to an archive
func (b *Backups) Add(name string, storageType string, size int64) (err error) {
	backup := Backup{Backups: b}

	// Get Name without suffix
	backup.Name = strings.TrimSuffix(name, backup.Extension)

	// Get extension
	backup.Extension = filepath.Ext(name)

	// Set size
	backup.Size = size

	// Set storage Type
	backup.StorageType = storageType

	// Get the time from backup name
	backupTimeRaw := ExtractTimeFromBackup.ReplaceAllString(backup.Name, "${1}")
	backup.Created, err = time.Parse(BackupTimeFormat, backupTimeRaw)
	if err == nil {
		// Add backup to the list of backups
		b.Backup = append(b.Backup, backup)
	}
	return err
}

// String returns an overview of the backups as string
func (b *Backups) String() (backups string) {
	var totalSize int64
	buf := new(bytes.Buffer)
	row := 0
	notSane := 0
	w := tabwriter.NewWriter(buf, 0, 0, 0, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "Backups")
	fmt.Fprintln(w, "# \tName \tExt \tSize \tStorage \t MinWAL \t Sane")
	for _, backup := range b.Backup {
		row++
		if !backup.IsSane() {
			notSane++
		}
		totalSize += backup.Size
		fmt.Fprintln(w, row,
			"\t", backup.Name,
			"\t", backup.Extension,
			"\t", humanize.Bytes(uint64(backup.Size)),
			"\t", backup.StorageType,
			"\t", backup.StartWalLocation,
			"\t", backup.IsSane())
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Total backups:", b.Len(), " Total size:",
		humanize.Bytes(uint64(totalSize)), " Not sane backups:", notSane)
	w.Flush()
	return buf.String()
}

// Backups implements sort.Interface based on Backup.Created
func (b *Backups) Len() int           { return len(b.Backup) }
func (b *Backups) Swap(i, j int)      { (b.Backup)[i], (b.Backup)[j] = (b.Backup)[j], (b.Backup)[i] }
func (b *Backups) Less(i, j int) bool { return (b.Backup)[i].Created.Before((b.Backup)[j].Created) }

// Sort sorts all backups in place
func (b *Backups) Sort() {
	// Sort, the newest first, newest to oldest
	// Sort should be relative cheap so it can be called on every change
	b.SortDesc()
}

// SortDesc sorts all backups in place DESC
func (b *Backups) SortDesc() {
	// Sort, the newest first
	sort.Sort(sort.Reverse(b))
}

// SortAsc sorts all backups in place ASC
func (b *Backups) SortAsc() {
	// Sort, the newest first
	sort.Sort(b)
}

// Find finds a backup by name and returns it
func (b *Backups) Find(name string) (backup *Backup, err error) {
	for _, backup := range b.Backup {
		if backup.Name == name {
			return &backup, nil
		}
	}
	return nil, errors.New("Backup not found: " + name)
}

// OldestBackup returns the oldest backup in the list
func (b *Backups) OldestBackup() (backup *Backup) {
	// Make sure the backups are sorted DESC
	b.SortDesc()

	if len(b.Backup) <= 0 {
		return nil
	}

	// Return the last (oldest) backup
	return &(b.Backup)[len(b.Backup)-1]
}

// NewestBackup returns the newest backup in the list
func (b *Backups) NewestBackup() (backup *Backup) {
	// Make sure the backups are sorted DESC
	b.SortDesc()

	if len(b.Backup) <= 0 {
		return nil
	}

	// Return the first (newest) backup
	return &(b.Backup)[0]
}

// SeparateBackupsByAge separates the backups by age
// The newest "countNew" backups are put in newBackups
// The older backups which are not already in newBackups are put in oldBackups
func (b *Backups) SeparateBackupsByAge(countNew uint) (newBackups Backups, oldBackups Backups, err error) {
	// Sort backups first
	b.SortDesc()

	// If there are not enough backups, return all as new
	if (*b).Len() < int(countNew) {
		return *b, Backups{}, errors.New("Not enough new backups")
	}

	oldBackups.Path = b.Path
	newBackups.Path = b.Path

	// Put the newest in newBackups
	newBackups.Backup = (b.Backup)[:countNew]

	// Put every other backup in oldBackups
	oldBackups.Backup = (b.Backup)[countNew:]

	if newBackups.IsSane() != true {
		return newBackups, oldBackups,
			errors.New("Not all backups (newBackups) are sane" + newBackups.String())
	}

	if newBackups.Len() <= 0 && oldBackups.Len() > 0 {
		panic("No new backups, only old. Not sane! ")
	}
	return newBackups, oldBackups, nil
}
