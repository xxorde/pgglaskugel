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
	"errors"
	"io/ioutil"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	"os"

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
		writeRecoveryConf := viper.GetBool("write-recovery-conf")
		force := viper.GetBool("force-restore")

		// If target directory does not exists ...
		if exists, err := pkg.Exists(backupDestination); !exists || err != nil {
			// ... create it
			err := os.MkdirAll(backupDestination, 0700)
			log.Fatal(err)
		}

		// If backup folder is not empty ask what to do
		if empty, err := pkg.IsEmpty(backupDestination); (!empty || err != nil) && force != true {
			force, err := pkg.AnswerConfirmation()
			if err != nil {
				log.Error(err)
			}

			if force != true {
				log.Fatal(backupDestination + " is not an empty directory, you need to use force")
			}
		}

		log.Info("Going to restore backup '", backupName, "' to: ", backupDestination)
		err := restoreBasebackup(backupDestination, backupName)
		if err != nil {
			log.Fatal(err)
		}

		if writeRecoveryConf {
			// When no restore_command command set, set it
			if viper.GetString("restore_command") == "" {
				// Include config file in potential restore_command command
				configOption := ""
				if viper.ConfigFileUsed() != "" {
					configOption = " --config " + viper.ConfigFileUsed()
				}

				// Preset restore_command
				viper.Set("restore_command", myExecutable+configOption+" recover %f %p")
			}
			restoreCommand := viper.GetString("restore_command")
			recoveryConf := "# Created by " + myExecutable + "\nrestore_command = '" + restoreCommand + "'"

			err = ioutil.WriteFile(backupDestination+"/recovery.conf", []byte(recoveryConf), 0600)
			if err != nil {
				panic(err)
			}
		}

		printDone()
	},
}

func restoreBasebackup(backupDestination string, backupName string) (err error) {
	backups := getMyBackups()

	backup, err := backups.Find(backupName)
	if err != nil {
		log.Fatal(err)
	}

	storageType := backup.StorageType()

	switch storageType {
	case "file":
		return restoreFromFile(backupDestination, backupName)
	case "s3":
		//		return restoreFromS3(backupDestination, backupName)
	default:
		log.Fatal(storageType, " no valid value for backup_to")
	}
	return errors.New("This should never be reached")
}

func restoreFromFile(backupDestination string, backupName string) (err error) {
	var backups pkg.Backups
	backups.GetBackupsInDir(viper.GetString("archivedir") + subDirBasebackup)

	backup, err := backups.Find(backupName)
	if err != nil {
		log.Fatal("Can not find backup: ", backupName)
	}

	inflateCmd := exec.Command(cmdZstd, "-d", "--stdout", backup.Path)
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
	return err
}

func init() {
	RootCmd.AddCommand(restoreCmd)
	restoreCmd.PersistentFlags().StringP("backup", "B", "myBackup@2016-11-04T21:52:57", "The backup to restore")
	restoreCmd.PersistentFlags().String("restore-to", "/var/lib/postgresql/pgGlaskugel-restore", "The destination to restore to")
	restoreCmd.PersistentFlags().Bool("write-recovery-conf", true, "Automatic create a recovery.conf to replay WAL from archive")
	restoreCmd.PersistentFlags().Bool("force-restore", false, "Force the deletion of existing data (danger zone)!")

	// Bind flags to viper
	viper.BindPFlag("backup", restoreCmd.PersistentFlags().Lookup("backup"))
	viper.BindPFlag("restore-to", restoreCmd.PersistentFlags().Lookup("restore-to"))
	viper.BindPFlag("write-recovery-conf", restoreCmd.PersistentFlags().Lookup("write-recovery-conf"))
	viper.BindPFlag("force-restore", restoreCmd.PersistentFlags().Lookup("force-restore"))
}
