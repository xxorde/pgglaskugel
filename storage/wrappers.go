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
	"path/filepath"

	"github.com/siddontang/go/log"
	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/util"
	"github.com/xxorde/pgglaskugel/wal"
)

// GetMyBackups does something
func GetMyBackups(viper func() map[string]interface{}) (backups util.Backups) {
	switch backend := viper()["backup_to"]; backend {
	case "s3":
		log.Debug("Get backups from S3")
		// Initialize minio client object.
		backups.MinioClient = GetS3Connection()
		s3bucketname := fmt.Sprintf("%v", viper()["s3_bucket_backup"])
		backups.GetBackupsInBucket(s3bucketname)
		s3bucketwal := fmt.Sprintf("%v", viper()["s3_bucket_wal"])
		backups.WalBucket = s3bucketwal
	// default == file
	default:
		log.Debug("Get backups from folder: ", viper()["backupDir"])
		backups.GetBackupsInDir(backupDir)
		backups.WalDir = filepath.Join(viper()["archivedir"], subDirWal)
	}
	return backups
}

// GetMyWals does something
func GetMyWals() (archive wal.Archive) {
	switch backend := viper()["backup_to"]; backend {
	case "s3":
		log.Debug("Get backups from S3")
		// Initialize minio client object.
		archive.MinioClient = storage.getS3Connection()
		archive.Bucket = viper.GetString("s3_bucket_wal")
	default:
		// Get WAL files from filesystem
		log.Debug("Get WAL from folder: ", walDir)
		archive.Path = walDir
	}
	archive.GetWals()
	return archive
}
