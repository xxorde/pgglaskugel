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

	"github.com/xxorde/pgglaskugel/backup"
)

// Backend is used to store and access data.
type Backend interface {

	// Writes a datastream to the given backend
	WriteStream(viper func() map[string]interface{}, input *io.Reader, name string, backuptype string)

	// Returns the data from the given backend
	Fetch(viper func() map[string]interface{}) error

	// Returns all found basebackups
	GetBasebackup(viper func() map[string]interface{}, backup *backup.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup)

	// Returns all found backups
	GetBackups(viper func() map[string]interface{}, subDirWal string) (backups backup.Backups)

	// Returns all found WAL-files
	GetWals(viper func() map[string]interface{}) (archive backup.Archive)

	// SeparateBackupsByAge separates the backups by age
	// The newest "countNew" backups are put in newBackups
	// The older backups which are not already in newBackups are put in oldBackups
	SeparateBackupsByAge(countNew uint, b *backup.Backups) (newBackups backup.Backups, oldBackups backup.Backups, err error)

	// DeleteAll deletes all backups in the struct
	DeleteAll(backups *backup.Backups) (count int, err error)
}
