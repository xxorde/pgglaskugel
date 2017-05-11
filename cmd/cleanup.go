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

package cmd

import (
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	log "github.com/Sirupsen/logrus"
	"github.com/xxorde/pgglaskugel/backup"
	"github.com/xxorde/pgglaskugel/storage"
	util "github.com/xxorde/pgglaskugel/util"
)

// cleanupCmd represents the cleanup command
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Deletes backups and WAL files enforcing an retention policy",
	Long: `Enforces your retention policy by deleting backups and WAL files.
	Use with care.`,
	Run: func(cmd *cobra.Command, args []string) {
		vipermap := viper.AllSettings
		backups := storage.GetMyBackups(vipermap, subDirWal)

		retain := uint(viper.GetInt("retain"))
		if retain <= 0 {
			log.Fatal("retain has to be 1 or higher! retain is: ", retain)
		}

		keep, discard, err := backups.SeparateBackupsByAge(retain)
		if err != nil {
			log.Error(err)
		}

		// Are all backups we want to keep sane?
		if keep.IsSane() != true {
			log.Warn("Not all backups to keep are sane, will only count sane backups for retention policy")
			log.Warn("The following backups will not count for retention policy: ", keep.Insane())

			// Keep only the sane backups
			// Not sane backups are not deleted, but they are not taken into account for retention policy
			// In sane backups will be deleted when too old
			keep = keep.Sane()
		}

		// Check if we have less backups than we want to keep
		if uint(keep.Len()) < retain {
			log.Warn("Not enough backups for retention policy!")
		}

		// Do we have backups to keep?
		if keep.Len() >= 1 {
			log.Info("Keep the following backups:", keep.String())
		} else {
			log.Info("No backups will be left!")
		}

		// Show backups to delete, or exit if none
		if discard.Len() >= 1 {
			log.Info("DELETE the following backups: ", discard.String())
		} else {
			log.Info("No backups will be removed!")
			os.Exit(0)
		}

		// The user must confirm deletion or set force-delete
		confirmDelete := viper.GetBool("force-delete")
		if confirmDelete != true {
			var err error
			confirmDelete, err = util.AnswerConfirmation("If you want to continue please type \"yes\" (Ctl-C to end):")
			util.CheckError(err)
		}
		if confirmDelete != true {
			log.Warn("Deletion was not confirmed, exiting now.")
			os.Exit(1)
		}

		// Delete all backups in the "discard" set
		count, err := storage.DeleteAll(vipermap, &discard)
		if err != nil {
			log.Warn("DeleteAll()", err)
		}
		log.Info(strconv.Itoa(count) + " backups were removed.")
		backups = storage.GetMyBackups(vipermap, subDirWal)

		// Show backups that are left
		log.Info("Backups left: " + backups.String())

		// Use oldest needed backup to determin oldest needed WAL file
		oldestBackup := backups.OldestBackup()
		oldestNeededWal, err := storage.GetStartWalLocation(vipermap, oldestBackup)
		if err != nil {
			log.Fatal(err)
		}
		if oldestNeededWal <= "" {
			log.Fatal("Could not get oldest needed WAL file")
		}
		log.Debug("oldestNeededWal: ", oldestNeededWal)

		// Create object to represent oldest needed WAL file
		var oldWal backup.Wal
		oldWal.Name = oldestNeededWal

		// Get all WAL files
		walArchive, err := storage.GetWals(vipermap)
		if err != nil {
			log.Error(err)
		}

		// Delete all WAL files that are older than oldestNeededWal
		count = storage.DeleteOldWal(vipermap, &walArchive, oldWal)
		log.Infof("Deleted %d WAL files:", count)

		printDone()
	},
}

func init() {
	RootCmd.AddCommand(cleanupCmd)
	cleanupCmd.PersistentFlags().Uint("retain", 0, "Number of (new) backups to keep?")
	cleanupCmd.PersistentFlags().Bool("force-delete", false, "Force the deletion of old backups, without asking!")

	// Bind flags to viper
	viper.BindPFlag("retain", cleanupCmd.PersistentFlags().Lookup("retain"))
	viper.BindPFlag("force-delete", cleanupCmd.PersistentFlags().Lookup("force-delete"))
}
