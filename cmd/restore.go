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
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	log "github.com/Sirupsen/logrus"
)

// restoreCmd represents the restore command
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore an existing backup to a given location",
	Long:  `Restore a given backup to a given location.`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug("restore called")
		checkNeededParameter("backup", "restore-to")
		backupName := viper.GetString("backup")
		backupDestination := viper.GetString("restore-to")

		force := viper.GetBool("force-restore")

		var backups pkg.Backups

		backups.GetBackupsInDir(viper.GetString("archivedir") + "/basebackup/")

		backup, err := backups.Find(backupName)
		if err != nil {
			log.Fatal("Can not find backup: ", backupName)
		}

		log.Info("Going to restore: ", backupName, " in: ", backup.Path, " to: ", backupDestination)

		if force != true {
			log.Info("If you want to continue please type \"yes\" (Ctl-C to end): ")
			var err error
			force, err = pkg.AnswerConfirmation()
			if err != nil {
				log.Error(err)
			}

			inflateCmd := exec.Command("zstd", "-d", "--stdout", backup.Path)
			untarCmd := exec.Command("tar", "--extract", "--directory", backupDestination)

			// attach pipe to the command
			inflateStdout, err := inflateCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Watch stderror
			inflateStderror, err := inflateCmd.StderrPipe()
			check(err)
			go pkg.WatchOutput(inflateStderror, log.Info)

			// Watch stderror
			untarStderror, err := untarCmd.StderrPipe()
			check(err)
			go pkg.WatchOutput(untarStderror, log.Info)

			// Pipe the backup in the compression
			untarCmd.Stdin = inflateStdout

			// Start untar
			if err := untarCmd.Start(); err != nil {
				log.Fatal("untarCmd failed on startup, ", err)
			}
			log.Info("Untar started")

			// Start inflate
			if err := inflateCmd.Start(); err != nil {
				log.Fatal("inflateCmd failed on startup, ", err)
			}
			log.Info("Inflation started")

			// Wait for backup to finish
			err = untarCmd.Wait()
			if err != nil {
				log.Fatal("untarCmd failed after startup, ", err)
			}
			log.Debug("untarCmd done")

			// Wait for compression to finish
			err = inflateCmd.Wait()
			if err != nil {
				log.Fatal("inflateCmd failed after startup, ", err)
			}
			log.Debug("inflateCmd done")
			printDone()
		}
	},
}

func init() {
	RootCmd.AddCommand(restoreCmd)
	restoreCmd.PersistentFlags().StringP("backup", "B", "myBackup@2016-11-04T21:52:57", "The backup to restore")
	restoreCmd.PersistentFlags().String("restore-to", "/var/lib/postgres/pgGlaskugel-restore", "The destination to restore to")
	restoreCmd.PersistentFlags().Bool("force-restore", false, "Force the deletion of existing data (danger zone)!")

	// Bind flags to viper
	viper.BindPFlag("backup", restoreCmd.PersistentFlags().Lookup("backup"))
	viper.BindPFlag("restore-to", restoreCmd.PersistentFlags().Lookup("restore-to"))
	viper.BindPFlag("force-restore", restoreCmd.PersistentFlags().Lookup("force-restore"))
}
