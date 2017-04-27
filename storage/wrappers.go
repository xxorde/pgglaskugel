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

package storage

import (
	"io"
	"log"
	"path/filepath"
	"sync"

	"github.com/xxorde/pgglaskugel/storage/local"
	"github.com/xxorde/pgglaskugel/storage/s3"
	"github.com/xxorde/pgglaskugel/util"
	"github.com/xxorde/pgglaskugel/wal"
)

// GetMyBackups does something
func GetMyBackups(viper func() map[string]interface{}, subDirWal string) (backups util.Backups) {
	switch backend := viper()["backup_to"]; backend {
	case "s3":
		return s3.GetBackups(viper, subDirWal)
	// default == file
	default:
		return local.GetBackups(viper, subDirWal)
	}
}

// GetMyWals does something
func GetMyWals(viper func() map[string]interface{}) (archive wal.Archive) {
	switch backend := viper()["backup_to"]; backend {
	case "s3":
		return s3.GetWals(viper)
	default:
		return local.GetWals(viper)
	}
}

// WriteStream writes the stream to the configured archive_to
func WriteStream(viper func() map[string]interface{}, input *io.Reader, name string, backuptype string) {
	var backuppath string
	if backuptype == "basebackup" {
		backuppath = filepath.Join(viper()["backupdir"].(string), name)
	} else if backuptype == "archive" {
		backuppath = filepath.Join(viper()["waldir"].(string), name)
	} else {
		log.Fatalf(" unknown stream-type: %s\n", backuptype)
	}
	switch backend := viper()["archive_to"]; backend {
	case "s3":
		s3.WriteStream(viper, input, name)
	default:
		local.WriteStream(input, backuppath)
	}
}

// Fetch fetches
func Fetch(viper func() map[string]interface{}, walTarget string, walName string) error {
	switch backend := viper()["archive_to"]; backend {
	case "s3":
		return s3.Fetch(viper, walTarget, walName)
	default:
		return local.Fetch(viper, walTarget, walName)
	}
}

// GetBasebackup gets basebackups
func GetBasebackup(viper func() map[string]interface{}, backup *util.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	// Add one worker to wait for finish
	wgDone.Add(1)

	storageType := backup.StorageType()
	switch storageType {
	case "s3":
		s3.Get(viper, backup, backupStream, wgStart, wgDone)
	default:
		local.Get(backup, backupStream, wgStart, wgDone)
	}
}
