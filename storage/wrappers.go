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
	"fmt"
	"io"
	"sync"

	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/backup"
	"github.com/xxorde/pgglaskugel/storage/backends/file"
	"github.com/xxorde/pgglaskugel/storage/backends/s3minioCs"

	log "github.com/Sirupsen/logrus"
)

var (
	// Definition in function below
	backends map[string]Backend
)

/*
 Storage Interface "Backend"" functions below
*/

// GetMyBackups does something
func GetMyBackups(viper *viper.Viper, subDirWal string) (backups backup.Backups) {
	bn := viper.GetString("backup_to")
	return backends[bn].GetBackups(viper, subDirWal)
}

// GetWals returns all Wal-Files for a Backup
func GetWals(viper *viper.Viper) (archive backup.Archive, err error) {
	bn := viper.GetString("backup_to")
	return backends[bn].GetWals(viper)
}

// WriteStream writes the stream to the configured archive_to
func WriteStream(viper *viper.Viper, input *io.Reader, name string, backuptype string) {
	bn := viper.GetString("backup_to")
	backends[bn].WriteStream(viper, input, name, backuptype)
}

// Fetch fetches
func Fetch(viper *viper.Viper) error {
	bn := viper.GetString("backup_to")
	return backends[bn].Fetch(viper)
}

// GetBasebackup gets basebackups
func GetBasebackup(viper *viper.Viper, bp *backup.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	bn := viper.GetString("backup_to")
	backends[bn].GetBasebackup(viper, bp, backupStream, wgStart, wgDone)
}

// DeleteAll deletes all backups in the struct
func DeleteAll(viper *viper.Viper, backups *backup.Backups) (count int, err error) {
	bn := viper.GetString("backup_to")
	return backends[bn].DeleteAll(viper, backups)
}

// GetStartWalLocation returns the oldest needed WAL file
// Every older WAL file is not required to use this backup
func GetStartWalLocation(viper *viper.Viper, bp *backup.Backup) (startWalLocation string, err error) {
	bn := viper.GetString("backup_to")
	return backends[bn].GetStartWalLocation(viper, bp)
}

// DeleteWal deletes the given WAL-file
func DeleteWal(viper *viper.Viper, w *backup.Wal) (err error) {
	bn := viper.GetString("backup_to")
	return backends[bn].DeleteWal(viper, w)
}

/*
	Not Interface functions below
*/

func init() {
	backends = initbackends()
}

func initbackends() map[string]Backend {
	fbackends := make(map[string]Backend)

	// The not included backends are not actively developed or tested!
	//var s3aws s3aws.S3backend
	//var s3minio s3minio.S3backend
	var s3minioCs s3minioCs.S3backend
	var file file.Localbackend
	//fbackends["s3aws"] = s3aws
	//fbackends["s3minio"] = s3minio
	fbackends["s3"] = s3minioCs
	fbackends["file"] = file
	return fbackends
}

// CheckBackend checks if the configured backend is supported
func CheckBackend(backend string) error {
	if _, ok := backends[backend]; ok {
		return nil
	}
	return fmt.Errorf("Backend %s not supported", backend)
}

// TODO Maybe we can move the function below to backup/wal.go. actually there is an import-circle

// DeleteOldWal deletes all WAL files that are older than lastWalToKeep
func DeleteOldWal(viper *viper.Viper, a *backup.Archive, lastWalToKeep backup.Wal) (deleted int) {
	// WAL files are deleted sequential
	// Due to the file system architecture parallel delete
	// Maybe this can be done in parallel for other storage systems
	visited := 0
	for _, wal := range a.WalFiles {
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
			err := DeleteWal(viper, &wal)
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
