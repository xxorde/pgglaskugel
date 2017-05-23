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

package storageInterface

import (
	"io"
	"sync"

	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/backup"
)

// Backend is used to store and access data.
type Backend interface {
	// New returns a newly initialized backend
	New(viper *viper.Viper) (newBackend Backend)

	// Writes a datastream to the given backend
	WriteStream(input *io.Reader, name string, backuptype string)

	// Returns the data from the given backend
	Fetch() error

	// Returns a specific basebackup
	GetBasebackup(backup *backup.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup)

	// Returns all basebackups
	GetBackups() (bp backup.Backups)

	// Returns all WAL-files
	GetWals() (backup.Archive, error)

	// DeleteAll deletes all backups in the struct
	DeleteAll(backups *backup.Backups) (count int, err error)

	// DeleteWal deletes the given WAL-file
	DeleteWal(w *backup.Wal) (err error)

	// DeleteWal deletes the given WAL-file
	DeleteOldWal(a *backup.Archive, lastWalToKeep backup.Wal) (deleted int)

	// Returns the first WAL-file name for a backup
	GetStartWalLocation(backup *backup.Backup) (startWalLocation string, err error)
}
