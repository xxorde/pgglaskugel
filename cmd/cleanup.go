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
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	log "github.com/Sirupsen/logrus"
)

// cleanupCmd represents the cleanup command
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Deletes backups and WAL files enforcing an retention policy",
	Long: `Enforces your retention policy by deleting backups and WAL files.
	Use with care.`,
	Run: func(cmd *cobra.Command, args []string) {
		backups := getMyBackups()

		retain := uint(viper.GetInt("retain"))
		if retain <= 0 {
			log.Fatal("retain has to be 1 or higher! retain is: ", retain)
		}

		keep, discard, err := backups.SeparateBackupsByAge(retain)
		if err != nil {
			log.Error(err)
		}

		if uint(keep.Len()) < retain {
			log.Warn("Not enough backups for retention policy!")
		}
		if keep.Len() >= 1 {
			log.Info("Keep the following backups:", keep.String())
		} else {
			log.Info("No backups will be left!")
		}

		if discard.Len() >= 1 {
			log.Info("DELETE the following backups: ", discard.String())
		} else {
			log.Info("No backups will be removed!")
			os.Exit(0)
		}

		confirmDelete := viper.GetBool("force-retain")

		if confirmDelete != true {
			log.Info("If you want to continue please type \"yes\" (Ctl-C to end): ")
			var err error
			confirmDelete, err = pkg.AnswerConfirmation()
			if err != nil {
				log.Error(err)
			}
		}

		if confirmDelete != true {
			log.Warn("Deletion was not confirmed, ending now.")
			os.Exit(1)
		}

		count, err := discard.DeleteAll()
		if err != nil {
			log.Warn("DeleteAll()", err)
		}
		log.Info(strconv.Itoa(count) + " backups were removed.")
		backups = getMyBackups()

		log.Info("Backups left: " + backups.String())
		oldestBackup := backups.OldestBackup()
		oldestNeededWal, err := oldestBackup.GetStartWalLocation(viper.GetString("archivedir") + "/wal")
		check(err)

		var oldWal pkg.Wal
		oldWal.Name = oldestNeededWal

		walArchiveFile := pkg.WalArchive{Path: viper.GetString("archivedir") + "/wal"}
		count, err = walArchiveFile.DeleteOldWal(oldWal)
		check(err)
		log.Infof("Deleted %d WAL files from: %s", count, viper.GetString("archivedir")+"/wal")

		// Clean WAL in S3
		bucketAwailable, err := backups.MinioClient.BucketExists(backups.WalBucket)
		if bucketAwailable && err == nil {
			walArchiveS3 := pkg.WalArchive{Bucket: backups.WalBucket, MinioClient: backups.MinioClient}
			count, err = walArchiveS3.DeleteOldWal(oldWal)
			log.Infof("Deleted %d WAL files from: s3://%s", count, backups.WalBucket)
			check(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(cleanupCmd)
	cleanupCmd.PersistentFlags().Uint("retain", 0, "Number of (new) backups to keep?")
	cleanupCmd.PersistentFlags().Bool("force-retain", false, "Force the deletion of old backups, without asking!")

	// Bind flags to viper
	viper.BindPFlag("retain", cleanupCmd.PersistentFlags().Lookup("retain"))
	viper.BindPFlag("force-retain", cleanupCmd.PersistentFlags().Lookup("force-retain"))
}
