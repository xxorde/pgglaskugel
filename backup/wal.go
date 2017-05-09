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
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"text/tabwriter"

	log "github.com/Sirupsen/logrus"
	humanize "github.com/dustin/go-humanize"
	"github.com/xxorde/pgglaskugel/util"
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

	if findBackupLabel.MatchString(wal.Name+wal.Extension) == true {
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
	fmt.Fprintln(w, "# \tName \tExt \tSize \tStorage \t Sane")
	for _, wal := range a.walFile {
		row++
		if !wal.IsSane() {
			notSane++
		}
		fmt.Fprintln(w, row, "\t", wal.Name, "\t", wal.Extension, "\t", humanize.Bytes(uint64(wal.Size)), "\t", wal.StorageType, "\t", wal.IsSane())
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Total WALs:", a.Len(), " Not sane WALs:", notSane)
	w.Flush()
	return buf.String()
}

// GetStartWalLocation returns the oldest needed WAL file
// Every older WAL file is not required to use this backup
func (b *Backup) GetStartWalLocation() (startWalLocation string, err error) {
	switch b.StorageType() {
	case "file":
		return b.GetStartWalLocationFromFile()
	case "s3":
		return b.GetStartWalLocationFromS3()
	}
	return "", errors.New("Not supported StorageType: " + b.StorageType())
}

// GetStartWalLocationFromFile returns the oldest needed WAL file
// Every older WAL file is not required to use this backup
func (b *Backup) GetStartWalLocationFromFile() (startWalLocation string, err error) {
	// Escape the name so we can use it in a regular expression
	searchName := regexp.QuoteMeta(b.Name)
	// Regex to identify the right file
	regLabel := regexp.MustCompile(`.*LABEL: ` + searchName)
	log.Debug("regLabel: ", regLabel)

	files, _ := ioutil.ReadDir(b.Backups.WalDir)
	// find all backup labels
	for _, f := range files {
		if f.Size() > maxBackupLabelSize {
			// size is to big for backup label
			continue
		}
		if regBackupLabelFile.MatchString(f.Name()) {
			log.Debug(f.Name(), " => seems to be a backup Label, by size and name")

			labelFile := b.Backups.WalDir + "/" + f.Name()
			catCmd := exec.Command("/usr/bin/zstdcat", labelFile)
			catCmdStdout, err := catCmd.StdoutPipe()
			if err != nil {
				// if we can not open the file we continue with next
				log.Warn("catCmd.StdoutPipe(), ", err)
				continue
			}

			err = catCmd.Start()
			if err != nil {
				log.Warn("catCmd.Start(), ", err)
				continue
			}

			buf, err := ioutil.ReadAll(catCmdStdout)
			if err != nil {
				log.Warn("Reading from command: ", err)
				continue
			}

			err = catCmd.Wait()
			if err != nil {
				log.Warn("catCmd.Wait(), ", err)
				continue
			}

			if len(regLabel.Find(buf)) > 1 {
				log.Debug("Found matching backup label file: ", f.Name())
				err = b.parseBackupLabel(buf)
				if err == nil {
					b.LabelFile = labelFile
				}
				return b.StartWalLocation, err
			}
		}
	}
	return "", errors.New("START WAL LOCATION not found")
}

// GetStartWalLocationFromS3 returns the oldest needed WAL file
// Every older WAL file is not required to use this backup
func (b *Backup) GetStartWalLocationFromS3() (startWalLocation string, err error) {
	// Escape the name so we can use it in a regular expression
	searchName := regexp.QuoteMeta(b.Name)
	// Regex to identify the right file
	regLabel := regexp.MustCompile(`.*LABEL: ` + searchName)
	log.Debug("regLabel: ", regLabel)

	log.Debug("Looking for the backup label that contains: ", searchName)

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	isRecursive := true
	objectCh := b.Backups.MinioClient.ListObjects(b.Backups.WalBucket, "", isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Error(object.Err)
		}

		// log.Debug("Looking at potential backup label: ", object.Key)

		if object.Size > maxBackupLabelSize {
			// size is to big for backup label
			// log.Debug("Object is to big to be a backup label, size: ", object.Size)
			continue
		}

		if regBackupLabelFile.MatchString(object.Key) {
			log.Debug(object.Key, " => seems to be a backup Label, by size and name")

			backupLabelFile, err := b.Backups.MinioClient.GetObject(b.Backups.WalBucket, object.Key)
			if err != nil {
				log.Warn("Can not get backupLabel, ", err)
				continue
			}

			bufCompressed := make([]byte, maxBackupLabelSize)
			readCount, err := backupLabelFile.Read(bufCompressed)
			if err != nil && err != io.EOF {
				log.Warn("Can not read backupLabel, ", err)
				continue
			}
			log.Debug("Read ", readCount, " from backupLabel")

			// Command to decompress the backuplabel
			catCmd := exec.Command("zstd", "-d", "--stdout")
			catCmdStdout, err := catCmd.StdoutPipe()
			if err != nil {
				// if we can not open the file we continue with next
				log.Warn("catCmd.StdoutPipe(), ", err)
				continue
			}

			// Use backupLabel as input for catCmd
			catDone := make(chan struct{}) // Channel to wait for WatchOutput
			catCmd.Stdin = bytes.NewReader(bufCompressed)
			catCmdStderror, err := catCmd.StderrPipe()
			go util.WatchOutput(catCmdStderror, log.Debug, catDone)

			err = catCmd.Start()
			if err != nil {
				log.Warn("catCmd.Start(), ", err)
				continue
			}

			bufPlain, err := ioutil.ReadAll(catCmdStdout)
			if err != nil {
				log.Warn("Reading from command: ", err)
				continue
			}

			// Wait for output watchers to finish
			// If the Cmd.Wait() is called while another process is reading
			// from Stdout / Stderr this is a race condition.
			// So we are waiting for the watchers first
			<-catDone

			// Wait for the command to finish
			err = catCmd.Wait()
			if err != nil {
				// We ignore errors here, zstd returns 1 even if everything is fine here
				log.Debug("catCmd.Wait(), ", err)
			}
			log.Debug("Backuplabel:\n", string(bufPlain))

			if len(regLabel.Find(bufPlain)) > 1 {
				log.Debug("Found matching backup label")
				err = b.parseBackupLabel(bufPlain)
				if err != nil {
					log.Error(err)
				}
				b.LabelFile = object.Key
				return b.StartWalLocation, err
			}
		}
	}
	return "", errors.New("START WAL LOCATION not found")
}

func (b *Backup) parseBackupLabel(backupLabel []byte) (err error) {
	regStartWalLine := regexp.MustCompile(`^START WAL LOCATION: .*\/.* \(file [0-9A-Fa-f]{24}\)`)
	regStartWal := regexp.MustCompile(`[0-9A-Fa-f]{24}`)

	startWalLine := regStartWalLine.Find(backupLabel)
	if len(startWalLine) < 1 {
		log.Debug(string(backupLabel))
		return errors.New("Can not find line with START WAL LOCATION")
	}

	startWal := regStartWal.Find(startWalLine)
	if len(startWal) < 1 {
		return errors.New("Can not find START WAL")
	}

	b.StartWalLocation = string(startWal)
	return nil
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
