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

	// WaitGroup for workers
	wg sync.WaitGroup

	basebackupCmd = &cobra.Command{
		Use:   "basebackup",
		Short: "Creates a new basebackup from the database",
		Long:  `Creates a new basebackup from the database with the given method.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("Perform basebackup")
			// Get time, name and path for basebackup
			backupTime := startTime.Format(util.BackupTimeFormat)
			backupName := clusterName + "@" + backupTime + ".zst"
			log.Info("Create new basebackup: ", backupName)

			conString := viper.GetString("connection")
			log.Debug("conString: ", conString)

			encrypt := viper.GetBool("encrypt")
			recipient := viper.GetString("recipient")

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
			backupStderror, err := backupCmd.StderrPipe()
			ec.Check(err)
			go util.WatchOutput(backupStderror, log.Info)

			// This command is used to compress the backup
			compressCmd := exec.Command(cmdZstd)

			// attach pipe to the command
			compressStdout, err := compressCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Watch output on stderror
			compressStderror, err := compressCmd.StderrPipe()
			ec.Check(err)
			go util.WatchOutput(compressStderror, log.Info)

			// Pipe the backup in the compression
			compressCmd.Stdin = backupStdout

			// Start the process (in the background)
			if err := backupCmd.Start(); err != nil {
				log.Fatal("pg_basebackup failed on startup, ", err)
			}
			log.Info("Backup was started")

			// Start compression
			if err := compressCmd.Start(); err != nil {
				log.Fatal("zstd failed on startup, ", err)
			}
			log.Info("Compression started")

			// Stream which is send to storage backend
			var backupStream io.Reader

			// Handle encryption
			var gpgCmd *exec.Cmd
			if encrypt {
				log.Debug("Encrypt data, encrypt: ", encrypt)
				// Encrypt the compressed data
				gpgCmd = exec.Command(cmdGpg, "--encrypt", "-o", "-", "--recipient", recipient)
				// Set the encryption output as input for S3
				var err error
				backupStream, err = gpgCmd.StdoutPipe()
				if err != nil {
					log.Fatal("Can not attach pipe to gpg process, ", err)
				}
				// Attach output of WAL to stdin
				gpgCmd.Stdin = compressStdout
				// Watch output on stderror
				gpgStderror, err := gpgCmd.StderrPipe()
				ec.Check(err)
				go util.WatchOutput(gpgStderror, log.Warn)

				// Start encryption
				if err := gpgCmd.Start(); err != nil {
					log.Fatal("gpg failed on startup, ", err)
				}
				log.Debug("gpg started")
			} else {
				// Do not use encryption
				backupStream = compressStdout
			}

			// Start worker
			// Add one worker to our waiting group (for waiting later)
			wg.Add(1)
			go handleBackupStream(&backupStream, backupName, &wg)

			// Wait for workers to finish
			//(WAIT FIRST FOR THE WORKER OR WE CAN LOOSE DATA)
			wg.Wait()

			// Wait for backup to finish
			// If there is still data in the output pipe it can be lost!
			log.Debug("Wait for backupCmd")
			err = backupCmd.Wait()
			if err != nil {
				log.Fatal("pg_basebackup failed after startup, ", err)
			}
			log.Debug("backupCmd done")

			// Wait for compression to finish
			// If there is still data in the output pipe it can be lost!
			log.Debug("Wait for compressCmd")
			err = compressCmd.Wait()
			if err != nil {
				log.Fatal("compression failed after startup, ", err)
			}
			log.Debug("compressCmd done")

			// If encryption is used wait for it to finish
			if encrypt {
				log.Debug("Wait for gpgCmd")
				err = gpgCmd.Wait()
				if err != nil {
					log.Fatal("gpg failed after startup, ", err)
				}
				log.Debug("Encryption done")
			}
			printDone()
		},
	}
)

// handleBackupStream takes a stream and persists it with the configured method
func handleBackupStream(input *io.Reader, filename string, wg *sync.WaitGroup) {
	// Tell the waiting group this process is done when function ends
	defer wg.Done()

	backupTo := viper.GetString("backup_to")
	switch backupTo {
	case "file":
		writeStreamToFile(input, filepath.Join(backupDir, filename))
	case "s3":
		writeStreamToS3(input, viper.GetString("s3_bucket_backup"), filename)
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
