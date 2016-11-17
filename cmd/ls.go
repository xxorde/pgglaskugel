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

package cmd

import (
	"time"

	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	log "github.com/Sirupsen/logrus"
)

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "Shows existing backups",
	Long:  `Shows you all backups already made with meta information.`,
	Run: func(cmd *cobra.Command, args []string) {
		showBackups()
		elapsed := time.Since(startTime)
		log.Info("Time to compleat: ", elapsed)
	},
}

func showBackups() {
	// TODO: Work your own magic here
	var backups pkg.Backups

	backupTo := viper.GetString("backup_to")
	switch backupTo {
	case "file":
		backupDir := archiveDir + "/basebackup"
		log.Debug("Get backups from folder: ", backupDir)
		backups.GetBackupsInDir(backupDir)
	case "s3":
		log.Debug("Get backups from S3")
		// Initialize minio client object.
		backups.MinioClient = getS3Connection()
		backups.GetBackupsInBucket(viper.GetString("s3_bucket_backup"))
	default:
		log.Fatal(backupTo, " no valid value for backupTo")
	}

	log.Info(backups.String())
}

func init() {
	RootCmd.AddCommand(lsCmd)
}
