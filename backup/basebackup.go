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
	"sort"
	"text/tabwriter"

	"github.com/dustin/go-humanize"
)

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

// String returns an overview of the backups as string
func (b *Backups) String() (backups string) {
	buf := new(bytes.Buffer)
	row := 0
	notSane := 0
	w := tabwriter.NewWriter(buf, 0, 0, 0, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "Backups")
	fmt.Fprintln(w, "# \tName \tExt \tSize \tStorage \t Sane")
	for _, backup := range b.Backup {
		row++
		if !backup.IsSane() {
			notSane++
		}
		fmt.Fprintln(w, row, "\t", backup.Name, "\t", backup.Extension, "\t", humanize.Bytes(uint64(backup.Size)), "\t", backup.StorageType(), "\t", backup.IsSane())
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Total backups:", b.Len(), " Not sane backups:", notSane)
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
