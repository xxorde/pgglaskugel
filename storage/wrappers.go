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
	"sync"

	"fmt"

	"github.com/xxorde/pgglaskugel/backup"
	"github.com/xxorde/pgglaskugel/storage/backends/local"
	"github.com/xxorde/pgglaskugel/storage/backends/s3"
)

func initbackends() map[string]Backend {
	backends := make(map[string]Backend)

	var s3b s3.S3backend
	var localb local.Localbackend
	backends["s3"] = s3b
	backends["file"] = localb
	return backends
}

// CheckBackend checks if the configured backend is supported
func CheckBackend(backend string) error {
	backends := initbackends()
	if _, ok := backends[backend]; ok {
		return nil
	}
	return fmt.Errorf("Backend %s not supported", backend)
}

// GetMyBackups does something
func GetMyBackups(viper func() map[string]interface{}, subDirWal string) (backups backup.Backups) {
	backends := initbackends()
	bn := viper()["backup_to"].(string)
	return backends[bn].GetBackups(viper, subDirWal)
}

// GetMyWals does something
func GetMyWals(viper func() map[string]interface{}) (archive backup.Archive) {
	backends := initbackends()
	bn := viper()["backup_to"].(string)
	return backends[bn].GetWals(viper)
}

// WriteStream writes the stream to the configured archive_to
func WriteStream(viper func() map[string]interface{}, input *io.Reader, name string, backuptype string) {
	backends := initbackends()
	bn := viper()["backup_to"].(string)
	backends[bn].WriteStream(viper, input, name, backuptype)

}

// Fetch fetches
func Fetch(viper func() map[string]interface{}) error {
	backends := initbackends()
	bn := viper()["backup_to"].(string)
	return backends[bn].Fetch(viper)
}

// GetBasebackup gets basebackups
func GetBasebackup(viper func() map[string]interface{}, backup *backup.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	backends := initbackends()
	// Add one worker to wait for finish
	wgDone.Add(1)

	storageType := backup.StorageType()
	backends[storageType].GetBasebackup(viper, backup, backupStream, wgStart, wgDone)
}

// SeparateBackupsByAge separates the backups by age
func SeparateBackupsByAge(viper func() map[string]interface{}, backups *backup.Backups, countNew uint) (newBackups backup.Backups, oldBackups backup.Backups, err error) {
	backends := initbackends()
	bn := viper()["backup_to"].(string)
	return backends[bn].SeparateBackupsByAge(countNew, backups)
}

// DeleteAll deletes all backups in the struct
func DeleteAll(viper func() map[string]interface{}, backups *backup.Backups) (count int, err error) {
	backends := initbackends()
	bn := viper()["backup_to"].(string)
	return backends[bn].DeleteAll(backups)
}
