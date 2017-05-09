// Package backup - static
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
	"regexp"
	"time"

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

	// RegFullWal - name of a WAL file
	RegFullWal = `^[0-9A-Za-z]{24}`
	// RegWalWithExt - name of a WAL file wit extension
	RegWalWithExt = `^([0-9A-Za-z]{24})(.*)`
	// RegTimeline - timeline of a given WAL file name
	RegTimeline = `^[0-9A-Za-z]{8}`
	// RegCounter segment counter in the given timeline
	RegCounter = `^([0-9A-Za-z]{8})([0-9A-Za-z]{16})`
	// RegBackupLabel backup label with any additional extension
	RegBackupLabel = `^[0-9A-Za-z]{24}\.[0-9A-Za-z]{8}\.backup.*`
	// BackupTimeFormat - time.RFC3339
	BackupTimeFormat  = time.RFC3339
	saneBackupMinSize = 2 * 1000000 // ~ 4MB

	// Larger files are most likely no backup label
	maxBackupLabelSize = 2048
)

var (
	nameFinder            = regexp.MustCompile(RegWalWithExt)  // *Regexp to extract the name from a WAL file with extension
	fulWalValidator       = regexp.MustCompile(RegFullWal)     // *Regexp to identify a WAL file
	timelineFinder        = regexp.MustCompile(RegTimeline)    // *Regexp to identify a timeline
	counterFinder         = regexp.MustCompile(RegCounter)     // *Regexp to get the segment counter
	findBackupLabel       = regexp.MustCompile(RegBackupLabel) // *Regexp to identify an backup label
	extractTimeFromBackup = regexp.MustCompile(`.*@`)          // Regexp to remove the name from a backup
	// Regex to identify a backup label file
	regBackupLabelFile = regexp.MustCompile(RegBackupLabel)
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

// Backups represents an array of "Backup"
type Backups struct {
	Backup      []Backup
	WalDir      string
	WalBucket   string
	MinioClient minio.Client
}

// Wal is a struct to represent a WAL file
type Wal struct {
	Name        string
	Extension   string
	Size        int64
	StorageType string
	Archive     *Archive
}

// Archive is a struct to represent an WAL archive
type Archive struct {
	walFile     []Wal
	Path        string
	Bucket      string
	MinioClient minio.Client
}
