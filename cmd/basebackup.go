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
	"io"
	"os/exec"
	"path/filepath"
	"sync"

	log "github.com/Sirupsen/logrus"
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	util "github.com/xxorde/pgglaskugel/util"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// Number of bytes to read per iteration
	nBytes = 64
)

var (
	basebackupCmd = &cobra.Command{
		Use:   "basebackup",
		Short: "Creates a new basebackup from the database",
		Long:  `Creates a new basebackup from the database with the given method.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("Perform basebackup")
			// Get time, name and path for basebackup
			backupTime := startTime.Format(util.BackupTimeFormat)
			backupName := clusterName + "@" + backupTime
			log.Info("Create new basebackup: ", backupName)

			// WaitGroup for workers
			var wg sync.WaitGroup

			conString := viper.GetString("connection")
			log.Debug("conString: ", conString)

			// Command to use pg_basebackup
			// Tar format, set backupName as label, make fast checkpoints, return output on standardout
			backupCmd := exec.Command("pg_basebackup", "--dbname", conString, "--format=tar", "--label", backupName, "--checkpoint", "fast", "--pgdata", "-")
			if viper.GetBool("no-standalone") == false {
				// Set command to include WAL files so the backup is usable without an archive
				backupCmd = exec.Command("pg_basebackup", "--dbname", conString, "--format=tar", "--label", backupName, "--checkpoint", "fast", "--pgdata", "-", "--xlog-method=fetch")
			}
			log.Debug("backupCmd: ", backupCmd)

			// attach pipe to the command
			backupStdout, err := backupCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Watch output on stderror
			backupDone := make(chan struct{}) // Channel to wait for WatchOutput
			backupStderror, err := backupCmd.StderrPipe()
			ec.Check(err)
			go util.WatchOutput(backupStderror, log.Info, backupDone)

			// Add one worker to our waiting group (for waiting later)
			wg.Add(1)

			// Start worker
			go compressEncryptStream(&backupStdout, backupName, storeBackupStream, &wg)

			// Start backup process (in the background)
			if err := backupCmd.Start(); err != nil {
				log.Fatal("pg_basebackup failed on startup, ", err)
			}
			log.Info("Backup was started")

			// Wait for output watchers to finish
			// If the Cmd.Wait() is called while another process is reading
			// from Stdout / Stderr this is a race condition.
			// So we are waiting for the watchers first
			log.Debug("Wait for <-backupDone")
			<-backupDone

			// Wait for workers to finish
			//(WAIT FOR THE WORKER FIRST OR WE CAN LOOSE DATA)
			log.Debug("Wait for wg.Wait()")
			wg.Wait()

			// Wait for backup to finish
			// If there is still data in the output pipe it can be lost!
			log.Debug("Wait for backupCmd.Wait()")
			err = backupCmd.Wait()
			if err != nil {
				log.Fatal("pg_basebackup failed after startup, ", err)
			}
			log.Debug("backupCmd done")

			printDone()
		},
	}
)

// handleBackupStream takes a stream and persists it with the configured method
func storeBackupStream(input *io.Reader, name string) {
	backupTo := viper.GetString("backup_to")
	switch backupTo {
	case "file":
		writeStreamToFile(input, filepath.Join(backupDir, name))
	case "s3":
		writeStreamToS3(input, viper.GetString("s3_bucket_backup"), name)
	default:
		log.Fatal(backupTo, " no valid value for backupTo")
	}
}

func init() {
	RootCmd.AddCommand(basebackupCmd)
	basebackupCmd.PersistentFlags().Bool("no-standalone", false, "Do not include WAL files in backup. If set all needed WAL files need to be available via the Archive! If set to false the archive is still needed for 'point in time recovery'!")
	// Bind flags to viper
	viper.BindPFlag("no-standalone", basebackupCmd.PersistentFlags().Lookup("no-standalone"))
}
