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
	"io"
	"io/ioutil"
	"os/exec"
	"regexp"
	"time"

	log "github.com/Sirupsen/logrus"
	wal "github.com/xxorde/pgglaskugel/wal"
)

const (
	BackupTimeFormat  = time.RFC3339
	saneBackupMinSize = 2 * 1000000 // ~ 4MB

	// Larger files are most likely no backup label
	maxBackupLabelSize = 2048
)

var (
	// Regex to identify a backup label file
	regBackupLabelFile = regexp.MustCompile(wal.RegBackupLabel)
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
	Backups          *Backups
}

// IsSane returns true if the backup seams sane
func (b *Backup) IsSane() (sane bool) {
	if b.Size < saneBackupMinSize {
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
		return "s3"
	}

	// Not defined
	return ""
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
			catCmd.Stdin = bytes.NewReader(bufCompressed)
			catCmdStderror, err := catCmd.StderrPipe()
			go WatchOutput(catCmdStderror, log.Debug, nil)

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
