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
package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"text/tabwriter"

	log "github.com/Sirupsen/logrus"
	humanize "github.com/dustin/go-humanize"
	minio "github.com/minio/minio-go"
)

const (
	// MaxWalSize maximum size a WAL can have
	MaxWalSize = int64(16777216)

	// MinArchiveSize minimal size for files to archive
	MinArchiveSize = int64(100)

	// StorageTypeFile represents an file backend
	StorageTypeFile = "file"
	// StorageTypeS3 represents an S3 backend
	StorageTypeS3 = "s3"

	// Regex to represent the ...
	regFullWal     = `^[0-9A-Za-z]{24}`                   // ... name of a WAL file
	regWalWithExt  = `^([0-9A-Za-z]{24})(.*)`             // ... name of a WAL file wit extension
	regTimeline    = `^[0-9A-Za-z]{8}`                    // ... timeline of a given WAL file name
	regCounter     = `^([0-9A-Za-z]{8})([0-9A-Za-z]{16})` // ... segment counter in the given timeline
	regBackupLabel = `^\.[0-9A-Za-z]{8}\.backup\..*`      // ... backup label
)

var (
	nameFinder      = regexp.MustCompile(regWalWithExt)  // *Regexp to extract the name from a WAL file with extension
	fulWalValidator = regexp.MustCompile(regFullWal)     // *Regexp to identify a WAL file
	counterFinder   = regexp.MustCompile(regCounter)     // *Regexp to get the segment counter
	findBackupLabel = regexp.MustCompile(regBackupLabel) // *Regexp to identify an backup label
)

// Wal is a struct to represent a WAL file
type Wal struct {
	Name        string
	Extension   string
	Size        int64
	StorageType string
	Archive     *Archive
}

// ImportName imports a WAL file by name (including extension)
func (w *Wal) ImportName(nameWithExtension string) (err error) {
	// Parses the input name and returns an array
	// 0 contains full string
	// 1 contains name
	// 2 contains extension
	nameRaw := nameFinder.FindStringSubmatch(nameWithExtension)
	// If not enough parameters are returned, parse was not possible
	if len(nameRaw) < 2 {
		return errors.New("WAL name does not parse: " + nameWithExtension)
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
	if fulWalValidator.MatchString(w.Name) {
		return true
	}
	return false
}

// Timeline returns the timeline of the WAL file
func (w *Wal) Timeline() (timeline string) {
	timelineFinder := regexp.MustCompile(regTimeline)
	timelineRaw := timelineFinder.Find([]byte(w.Name))

	timeline = string(timelineRaw)
	return timeline
}

// Counter returns the counter / postion in the current timeline
func (w *Wal) Counter() (counter string) {
	counterRaw := counterFinder.FindStringSubmatch(w.Name)

	// 0 contains full string
	// 1 contains timeline
	// 2 contains counter
	counter = string(counterRaw[2])
	return counter
}

// OlderThan returns if *Wal is older than newWal
func (w *Wal) OlderThan(newWal Wal) (isOlderThan bool, err error) {
	if w.IsSane() != true {
		return false, errors.New("WAL not sane: " + w.Name)
	}

	if newWal.IsSane() != true {
		return false, errors.New("WAL not sane: " + newWal.Name)
	}

	if newWal.Name > w.Name {
		return true, nil
	}
	return false, nil
}

// Delete delets the WAL file
func (w *Wal) Delete() (err error) {
	switch w.StorageType {
	case StorageTypeFile:
		err = os.Remove(filepath.Join(w.Archive.Path, w.Name+w.Extension))
		if err != nil {
			log.Warn(err)
		}
		return err
	case StorageTypeS3:
		err = w.Archive.MinioClient.RemoveObject(w.Archive.Bucket, w.Name+w.Extension)
	default:
		return errors.New("Not supported StorageType: " + w.StorageType)
	}
	if err != nil {
		log.Warn(err)
	}
	return err
}

// Archive is a struct to represent an WAL archive
type Archive struct {
	walFile     []Wal
	Path        string
	Bucket      string
	MinioClient minio.Client
}

// GetWals adds all WAL files in known backends
func (a *Archive) GetWals() (loadCounter int, err error) {
	loadCounterFile := 0
	loadCounterS3 := 0

	// Use path to get WAL files if set
	if a.Path > "" {
		loadCounterFile, err = a.GetWalsInDir(a.Path)
	}
	if err != nil {
		log.Warn(err)
	}

	// Use bucket to get WAL files if set
	if a.Bucket > "" {
		loadCounterS3, err = a.GetWalsInBucket(a.Bucket)
	}
	if err != nil {
		log.Warn(err)
	}
	return loadCounterFile + loadCounterS3, nil
}

// GetWalsInDir adds all WAL files in walDir to the archive
func (a *Archive) GetWalsInDir(walDir string) (loadCounter int, err error) {
	// WAL files are load sequential from file system.
	files, err := ioutil.ReadDir(a.Path)
	if err != nil {
		log.Warn(err)
	}
	for _, f := range files {
		size := f.Size()
		err = a.Add(f.Name(), StorageTypeFile, size)
		if err != nil {
			log.Warn(err)
			continue
		}
		loadCounter++
	}
	return loadCounter, nil
}

// GetWalsInBucket includes all WALs in given bucket
func (a *Archive) GetWalsInBucket(bucket string) (loadCounter int, err error) {
	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	isRecursive := true
	objectCh := a.MinioClient.ListObjects(bucket, "", isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Error(object.Err)
		}
		log.Debug(object)
		if object.Err != nil {
			log.Error(object.Err)
		}

		err = a.Add(object.Key, StorageTypeS3, object.Size)
		if err != nil {
			log.Warn(err)
			return loadCounter, err
		}
		loadCounter++

	}
	return loadCounter, err
}

// Add adds an WAL to an archive
func (a *Archive) Add(name string, storageType string, size int64) (err error) {
	wal := Wal{Archive: a}
	err = wal.ImportName(name)
	if err != nil {
		return err
	}

	if findBackupLabel.MatchString(wal.Extension) == true {
		// This is a backup label not a WAL file
		// We should we add it as well till we come up with a better solution :)
	}

	// Set storage Type
	wal.StorageType = storageType

	// Set size
	wal.Size = size

	// Append WAL file to archive
	a.walFile = append(a.walFile, wal)
	return nil
}

// DeleteOldWal deletes all WAL files that are older than lastWalToKeep
func (a *Archive) DeleteOldWal(lastWalToKeep Wal) (deleted int) {
	// WAL files are deleted sequential
	// Due to the file system architecture parallel delete
	// Maybe this can be done in parallel for other storage systems
	visited := 0
	for _, wal := range a.walFile {
		// Count up
		visited++

		// Check if current visited WAL is older than lastWalToKeep
		old, err := wal.OlderThan(lastWalToKeep)
		if err != nil {
			log.Warn(err)
			continue
		}

		// If it is older, delete it
		if old {
			log.Debugf("Older than %s => going to delete: %s", lastWalToKeep.Name, wal.Name)
			err := wal.Delete()
			if err != nil {
				log.Warn(err)
				continue
			}
			deleted++
		}
	}
	log.Debugf("Checked %d files and deleted %d", visited, deleted)
	return deleted
}

// StorageTypeFile returns true if archive has file backend
func (a *Archive) StorageTypeFile() (hasS3 bool) {
	if a.Path > "" {
		return true
	}
	return false
}

// StorageTypeS3 returns true if archive has S3 backend
func (a *Archive) StorageTypeS3() (hasS3 bool) {
	if a.Bucket > "" {
		return true
	}
	return false
}

// String returns an overview of the backups as string
func (a *Archive) String() (archive string) {
	buf := new(bytes.Buffer)
	row := 0
	notSane := 0
	w := tabwriter.NewWriter(buf, 0, 0, 0, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "WAL filesbackup in archive")
	fmt.Fprintln(w, "# \tName \tExt \tSize \t Sane")
	for _, wal := range a.walFile {
		row++
		if !wal.IsSane() {
			notSane++
		}
		fmt.Fprintln(w, row, "\t", wal.Name, "\t", wal.Extension, "\t", humanize.Bytes(uint64(wal.Size)), "\t", wal.IsSane())
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Total WALs:", a.Len(), " Not sane WALs:", notSane)
	w.Flush()
	return buf.String()
}

// Archive implements sort.Interface based on Backup.Created
func (a *Archive) Len() int           { return len(a.walFile) }
func (a *Archive) Swap(i, j int)      { (a.walFile)[i], (a.walFile)[j] = (a.walFile)[j], (a.walFile)[i] }
func (a *Archive) Less(i, j int) bool { return (a.walFile)[i].Name < (a.walFile)[j].Name }

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
