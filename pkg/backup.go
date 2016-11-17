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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/dustin/go-humanize"
	minio "github.com/minio/minio-go"
)

const (
	BackupTimeFormat = "2006-01-02T15:04:05"
	saneMinSize      = 4 * 1000000 // ~ 4MB
)

// Backup stores information about a backup
type Backup struct {
	Name             string
	Extension        string
	Path             string
	Bucket           string
	Size             int64
	Created          time.Time
	LabelFile        string
	BackupLabel      string
	StartWalLocation string
}

// IsSane returns true if the backup seams sane
func (b *Backup) IsSane() (sane bool) {
	if b.Size < saneMinSize {
		return false
	}

	if b.StorageType() == "" {
		return false
	}

	return true
}

// StorageType returns the type of storage the backup is on
func (b *Backup) StorageType() (storageType string) {
	if b.Path > "" {
		return "file"
	}

	if b.Bucket > "" {
		return "S3"
	}

	// Not defined
	return ""
}

// GetStartWalLocation returns the oldest needed WAL file
// Every older WAL file is not required to use this backup
func (b *Backup) GetStartWalLocation(archiveDir string) (startWalLocation string, err error) {
	// Regex to identify a backup label file
	// 000000010000001200000062.00000028.backup, make better regex
	regBackupLabelFile := regexp.MustCompile(`.*\.backup`)

	// Regex to identify the right file
	regLabel := regexp.MustCompile(`.*LABEL: ` + b.Name)

	files, _ := ioutil.ReadDir(archiveDir)
	// find all backup labels
	for _, f := range files {
		if f.Size() > 500 {
			// size is to big for backup label
			continue
		}
		if regBackupLabelFile.MatchString(f.Name()) {
			log.Debug(f.Name(), " => seems to be a backup Label, by size and name")

			labelFile := archiveDir + "/" + f.Name()
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
				log.Debug("Found matching backup label")
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

// GetLabelFile returns the label file of a backup
func (b *Backup) GetLabelFile(archiveDir string) (labelFile string, err error) {
	_, err = b.GetStartWalLocation(archiveDir)
	if err != nil {
		return "", err
	}
	return b.LabelFile, nil
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
		return errors.New("Can not find START WAL.")
	}

	b.StartWalLocation = string(startWal)
	return nil
}

// Backups represents an array of "Backup"
type Backups struct {
	Backup      []Backup
	MinioClient minio.Client
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

// AddFile adds a new backup to Backups
func (b *Backups) AddFile(path string) (err error) {
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
	if err != nil {
		return err
	}
	defer file.Close()

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
	b.Backup = append(b.Backup, newBackup)
	return nil
}

// AddObject adds a new backup to Backups
func (b *Backups) AddObject(object minio.ObjectInfo, bucket string) (err error) {
	var newBackup Backup
	newBackup.Bucket = bucket
	newBackup.Extension = filepath.Ext(object.Key)

	// Get Name without suffix
	newBackup.Name = strings.TrimSuffix(object.Key, newBackup.Extension)
	newBackup.Size = object.Size

	// Remove anything before the '@'
	reg := regexp.MustCompile(`.*@`)
	backupTimeRaw := reg.ReplaceAllString(newBackup.Name, "${1}")
	newBackup.Created, err = time.Parse(BackupTimeFormat, backupTimeRaw)
	if err != nil {
		return err
	}
	b.Backup = append(b.Backup, newBackup)
	return nil
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

// GetBackupsInDir includes all backups in given directory
func (b *Backups) GetBackupsInDir(backupDir string) {
	files, _ := ioutil.ReadDir(backupDir)
	for _, f := range files {
		err := b.AddFile(backupDir + "/" + f.Name())
		if err != nil {
			log.Warn(err)
		}
	}
	// Sort backups
	b.Sort()
}

// GetBackupsInBucket includes all backups in given bucket
func (b *Backups) GetBackupsInBucket(bucket string) {
	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	isRecursive := true
	objectCh := b.MinioClient.ListObjects(bucket, "", isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Error(object.Err)
		}
		log.Debug(object)
		err := b.AddObject(object, bucket)
		if err != nil {
			log.Error(err)
		}
	}

	// Sort backups
	b.Sort()
}

// Backups implements sort.Interface for []Person based on Backup.Created
func (b *Backups) Len() int           { return len(b.Backup) }
func (b *Backups) Swap(i, j int)      { (b.Backup)[i], (b.Backup)[j] = (b.Backup)[j], (b.Backup)[i] }
func (b *Backups) Less(i, j int) bool { return (b.Backup)[i].Created.Before((b.Backup)[j].Created) }

// SortDesc sorts all backups in place
func (b *Backups) Sort() {
	// Sort, the newest first
	b.SortDesc()
}

// SortDesc sorts all backups in place
func (b *Backups) SortDesc() {
	// Sort, the newest first
	sort.Sort(sort.Reverse(b))
}

// SortAsc sorts all backups in place
func (b *Backups) SortAsc() {
	// Sort, the newest first
	sort.Sort(b)
}

// SeparateBackupsByAge separates the backups by age
// The newest "countNew" backups are put in newBackups
// The older backups are put in oldBackups
func (b *Backups) SeparateBackupsByAge(countNew uint) (newBackups Backups, oldBackups Backups, err error) {
	// Sort backups first
	b.Sort()

	// If there are not enough backups, return all
	if (*b).Len() <= int(countNew) {
		return *b, Backups{}, nil
	}

	// Give the additional vars to the ne sets
	newBackups.MinioClient = b.MinioClient
	oldBackups.MinioClient = b.MinioClient

	// Putt the newest in newBackups
	newBackups.Backup = (b.Backup)[:countNew]
	oldBackups.Backup = (b.Backup)[countNew:]

	if newBackups.IsSane() != true {
		return Backups{}, Backups{}, errors.New("Not all backups (newBackups) are sane!" + newBackups.String())
	}

	if newBackups.Len() <= 0 && oldBackups.Len() > 0 {
		panic("No new backups, only old. Not sane! ")
	}
	return newBackups, oldBackups, nil
}

// DeleteAll deletes all backups in the struct
func (b *Backups) DeleteAll() (count int, err error) {
	// We delete all backups, but start with the oldest just in case
	b.SortAsc()
	for _, backup := range b.Backup {
		if backup.Path != "" {
			err = os.Remove(backup.Path)
			if err != nil {
				log.Warn(err)
			} else {
				count++
			}
		}
		if backup.Bucket != "" {
			err = b.MinioClient.RemoveObject(backup.Bucket, backup.Name+backup.Extension)
			if err != nil {
				log.Warn(err)
			} else {
				count++
			}
		}
	}
	return count, err
}

// Find finds a backup by name and returns is
func (b *Backups) Find(name string) (backup *Backup, err error) {
	for _, backup := range b.Backup {
		if backup.Name == name {
			return &backup, nil
		}
	}
	return nil, errors.New("Backup not found: " + name)
}
