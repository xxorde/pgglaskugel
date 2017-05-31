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
	// WalWal identifies a standart WAL
	WalWal = WalType(0)
	// WalBackuplabel identifies a WAL as backuplabel
	WalBackuplabel = WalType(1)
	// WalHistory identifies a WAL as history files
	WalHistory = WalType(2)

	// MaxWalSize maximum size a WAL can have
	MaxWalSize = int64(16777216)
	// MinArchiveSize minimal size for files to archive
	MinArchiveSize = int64(100)
	// RegFullWal - name of a WAL file
	RegFullWal = `^[0-9A-Za-z]{24}`
	// RegWalWithExt - name of a WAL file with extension
	RegWalWithExt = `^([0-9A-Za-z]{24})(.*)`
	// RegHistoryWithExt - history file with extension
	RegHistoryWithExt = `^([0-9A-Za-z]{8}\.history)(.*)`
	// RegTimeline - timeline of a given WAL file name
	RegTimeline = `^[0-9A-Za-z]{8}`
	// RegCounter segment counter in the given timeline
	RegCounter = `^([0-9A-Za-z]{8})([0-9A-Za-z]{16})`
	// RegBackupLabel backup label with any additional extension
	RegBackupLabel = `^[0-9A-Za-z]{24}\.[0-9A-Za-z]{8}\.backup.*`
	// RegHistory history file with any additional extension
	RegHistory = `^[0-9A-Za-z]{8}\.history.*`
	// BackupTimeFormat - time.RFC3339
	BackupTimeFormat = time.RFC3339
	// SaneBackupMinSize - min size for backups
	SaneBackupMinSize = 1024 * 1024 * 2 // 2MB
	// MaxBackupLabelSize Larger files are most likely no backup label
	MaxBackupLabelSize = 4096
)

var (
	nameFinder      = regexp.MustCompile(RegWalWithExt)     // *Regexp to extract the name from a WAL file with extension
	historyFinder   = regexp.MustCompile(RegHistoryWithExt) // *Regexp to extract the name from a history file with extension
	fulWalValidator = regexp.MustCompile(RegFullWal)        // *Regexp to identify a WAL file
	timelineFinder  = regexp.MustCompile(RegTimeline)       // *Regexp to identify a timeline
	counterFinder   = regexp.MustCompile(RegCounter)        // *Regexp to get the segment counter
	findBackupLabel = regexp.MustCompile(RegBackupLabel)    // *Regexp to identify an backup label
	findHistory     = regexp.MustCompile(RegHistory)        // *Regexp to identify an history file

	// RegBackupLabelFile Regex to identify a backup label file
	RegBackupLabelFile = regexp.MustCompile(RegBackupLabel)
)

// Backup stores information about a backup
type Backup struct {
	Name      string
	Extension string
	// Path is also used for alternative backup paths (e.g. bucket in S3)
	Path             string
	Size             int64
	Created          time.Time
	LabelFile        string
	BackupLabel      string
	StartWalLocation string
	StorageType      string
	Backups          *Backups
}

// Backups represents an array of "Backup"
type Backups struct {
	Name   string
	Backup []Backup
	// WalPath is also used for alternative backup paths (e.g. bucket in S3)
	WalPath string
}

// Wal is a struct to represent a WAL file
type Wal struct {
	Name        string
	Extension   string
	Size        int64
	StorageType string
	Type        WalType
	Archive     *Archive
}

// WalType represents different types of WAL
type WalType uint8

// Archive is a struct to represent an WAL archive
type Archive struct {
	WalFiles    []Wal
	Path        string
	Bucket      string
	MinioClient minio.Client
}
