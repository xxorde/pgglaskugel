// Package backup - wal module
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

	log "github.com/Sirupsen/logrus"
	humanize "github.com/dustin/go-humanize"
)

// ImportName imports a WAL file by name (including extension)
func (w *Wal) ImportName(nameWithExtension string) (err error) {
	// Parses the input name and returns an array
	// 0 contains full string
	// 1 contains name
	// 2 contains extension
	nameRaw := nameFinder.FindStringSubmatch(nameWithExtension)
	// If not enough parameters are returned, parse was not possible
	if len(nameRaw) < 2 {
		log.Debug("nameFinder nameRaw ", nameRaw)
		// Name does not parse as WAL / backup label
		// There is still a chance it is an history file which occurs after timeline switch

		// Parses the input name as history and returns an array
		// 0 contains full string
		// 1 contains name
		// 2 contains extension
		nameRaw = historyFinder.FindStringSubmatch(nameWithExtension)
		if len(nameRaw) < 2 {
			log.Debug("historyFinder nameRaw ", nameRaw)
			return errors.New("WAL name does not parse: " + nameWithExtension)
		}
		log.Debug("History file found: ", nameWithExtension)
		w.Type = WalHistory
	} else {
		if findBackupLabel.MatchString(nameWithExtension) == true {
			// We found a Backuplabel
			w.Type = WalBackuplabel
		} else {
			// We found a standsad WAL
			w.Type = WalWal
		}
	}

	w.Name = string(nameRaw[1])
	w.Extension = string(nameRaw[2])
	return nil
}

// IsSane returns if the WAL file seems sane
func (w *Wal) IsSane() (sane bool) {
	return w.SaneName()
}

// SaneName returns if the WAL file name seems sane
func (w *Wal) SaneName() (saneName bool) {
	// If name looks like WAL file
	if fulWalValidator.MatchString(w.Name) {
		return true
	}

	// If type is WalHistory and name looks like history file
	if w.Type == WalHistory && findHistory.MatchString(w.Name) {
		return true
	}
	return false
}

// Timeline returns the timeline of the WAL file
func (w *Wal) Timeline() (timeline string) {
	timelineRaw := timelineFinder.Find([]byte(w.Name))

	timeline = string(timelineRaw)
	return timeline
}

// Counter returns the counter / position in the current timeline
func (w *Wal) Counter() (counter string) {
	counterRaw := counterFinder.FindStringSubmatch(w.Name)

	// 0 contains full string
	// 1 contains timeline
	// 2 contains counter
	counter = string(counterRaw[2])
	return counter
}

// OlderThan returns if *Wal is older than newWal
func (w *Wal) OlderThan(newWal Wal) (isOlderThan bool) {
	if newWal.Name > w.Name {
		return true
	}
	return false
}

// Add adds an WAL to an archive
func (a *Archive) Add(name string, storageType string, size int64) (err error) {
	wal := Wal{Archive: a}
	err = wal.ImportName(name)
	if err != nil {
		return err
	}

	// Set storage Type
	wal.StorageType = storageType

	// Set size
	wal.Size = size

	// Test WAL
	if wal.IsSane() != true {
		return errors.New("WAL not sane: " + wal.Name)
	}

	// Append WAL file to archive
	a.WalFiles = append(a.WalFiles, wal)
	return nil
}

// String returns an overview of the backups as string
func (a *Archive) String() (archive string) {
	var totalSize int64
	buf := new(bytes.Buffer)
	row := 0
	notSane := 0
	w := tabwriter.NewWriter(buf, 0, 0, 0, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "WAL filesbackup in archive")
	fmt.Fprintln(w, "# \t Name \t Ext \t Size \t Type \t Storage \t Sane")
	for _, wal := range a.WalFiles {
		row++
		if !wal.IsSane() {
			notSane++
		}
		totalSize += wal.Size
		fmt.Fprintln(w, row,
			"\t", wal.Name,
			"\t", wal.Extension,
			"\t", humanize.Bytes(uint64(wal.Size)),
			"\t", wal.Type,
			"\t", wal.StorageType,
			"\t", wal.IsSane())
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Total WALs:", a.Len(), " Total size:",
		humanize.Bytes(uint64(totalSize)), " Not sane WALs:", notSane)
	w.Flush()
	return buf.String()
}

// Archive implements sort.Interface based on Backup.Created
func (a *Archive) Len() int           { return len(a.WalFiles) }
func (a *Archive) Swap(i, j int)      { (a.WalFiles)[i], (a.WalFiles)[j] = (a.WalFiles)[j], (a.WalFiles)[i] }
func (a *Archive) Less(i, j int) bool { return (a.WalFiles)[i].Name < (a.WalFiles)[j].Name }

// Sort sorts all backups in place
func (a *Archive) Sort() {
	// Sort, the newest first, newest to oldest
	// Sort should be relative cheap so it can be called on every change
	a.SortDesc()
}

// SortDesc sorts all backups in place DESC
func (a *Archive) SortDesc() {
	// Sort, the newest first
	sort.Sort(sort.Reverse(a))
}

// SortAsc sorts all backups in place ASC
func (a *Archive) SortAsc() {
	// Sort, the newest first
	sort.Sort(a)
}

func (w WalType) String() (typeString string) {
	switch w {
	case WalWal:
		return "WAL"
	case WalBackuplabel:
		return "label"
	case WalHistory:
		return "history"
	default:
		return "type not defined: " + string(uint(w))
	}
}
