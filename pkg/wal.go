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
	"errors"
	"io/ioutil"
	"os"
	"regexp"

	log "github.com/Sirupsen/logrus"
	minio "github.com/minio/minio-go"
)

const (
	// MaxWalSize maximum size a WAL can have
	MaxWalSize = int64(16777216)

	// MinArchiveSize minimal size for files to archive
	// WAL min size would be equal to maxWalSize
	// but backup labels are archived too.
	MinArchiveSize = int64(100)

	regFullWal  = `^[0-9A-Za-z]{24}`
	regTimeline = `^[0-9A-Za-z]{8}`
	regCounter  = `^([0-9A-Za-z]{8})([0-9A-Za-z]{16})`
)

type Wal struct {
	Name       string
	Extension  string
	Size       int64
	WalArchive *WalArchive
}

func (w *Wal) Timeline() (timeline string) {
	timelineFinder := regexp.MustCompile(regTimeline)
	timelineRaw := timelineFinder.Find([]byte(w.Name))

	timeline = string(timelineRaw)
	return timeline
}

func (w *Wal) Counter() (counter string) {
	counterFinder := regexp.MustCompile(regCounter)
	counterRaw := counterFinder.FindStringSubmatch(w.Name)

	// 0 contains full string
	// 1 contains timeline
	// 2 contains counter
	counter = string(counterRaw[2])
	return counter
}

func (w *Wal) OlderThan(newWal Wal) (isOlderThan bool, err error) {
	if w.Timeline() != newWal.Timeline() {
		return false, errors.New("Timeline does not match")
	}
	if newWal.Counter() > w.Counter() {
		return true, nil
	}
	return false, nil
}

func (w *Wal) Delete() (err error) {
	switch w.WalArchive.StorageType() {
	case "file":
		err = os.Remove(w.WalArchive.Path + "/" + w.Name)
		if err != nil {
			log.Warn(err)
		}
		return err
	case "s3":
		err = w.WalArchive.MinioClient.RemoveObject(w.WalArchive.Bucket, w.Name)
	default:
		return errors.New("Not supported StorageType: " + w.WalArchive.StorageType())
	}
	if err != nil {
		log.Warn(err)
	}
	return err
}

type WalArchive struct {
	walFile     []Wal
	Path        string
	Bucket      string
	MinioClient minio.Client
}

// StorageType returns the type of storage the backup is on
func (w *WalArchive) StorageType() (storageType string) {
	if w.Path > "" {
		return "file"
	}
	if w.Bucket > "" {
		return "s3"
	}
	// Not defined
	return ""
}

func (w *WalArchive) DeleteOldWalFromFile(lastWalToKeep Wal) (count int, err error) {
	files, _ := ioutil.ReadDir(w.Path)
	for _, f := range files {
		wal := Wal{Name: f.Name(), WalArchive: w}
		old, err := wal.OlderThan(lastWalToKeep)
		if err != nil {
			log.Warn(err)
			continue
		}
		if old {
			log.Debugf("%s older than %s", wal.Name, lastWalToKeep.Name)
			err := wal.Delete()
			if err != nil {
				log.Warn(err)
				continue
			}
			count++
		}
	}
	return count, nil
}

func (w *WalArchive) DeleteOldWalFromS3(lastWalToKeep Wal) (count int, err error) {
	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	isRecursive := true
	objectCh := w.MinioClient.ListObjects(w.Bucket, "", isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Error(object.Err)
		}
		wal := Wal{Name: object.Key, WalArchive: w}
		old, err := wal.OlderThan(lastWalToKeep)
		if err != nil {
			log.Warn(err)
			continue
		}
		if old {
			log.Debugf("%s older than %s", wal.Name, lastWalToKeep.Name)
			err := wal.Delete()
			if err != nil {
				log.Warn(err)
				continue
			}
			count++
		}
	}
	return count, nil
}

func (w *WalArchive) DeleteOldWal(lastWalToKeep Wal) (count int, err error) {
	switch w.StorageType() {
	case "file":
		return w.DeleteOldWalFromFile(lastWalToKeep)
	case "s3":
		return w.DeleteOldWalFromS3(lastWalToKeep)
	default:
		return 0, errors.New("Not supported StorageType: " + w.StorageType())
	}
}
