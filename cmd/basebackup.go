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
	"os"
	"os/exec"
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
			backupName := "bb@" + backupTime + ".zst"
			conString := viper.GetString("connection")
			encrypt := viper.GetBool("encrypt")
			recipient := viper.GetString("recipient")
			log.Debug("conString: ", conString)

			// Command to use pg_basebackup
			// Tar format, set backupName as label, make fast checkpoints, return output on standardout
			backupCmd := exec.Command("pg_basebackup", "--dbname", conString, "--format=tar", "--label", backupName, "--checkpoint", "fast", "--pgdata", "-")
			if viper.GetBool("standalone") {
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

			// This command is used to take the backup and compress it
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
			err = backupCmd.Wait()
			if err != nil {
				log.Fatal("pg_basebackup failed after startup, ", err)
			}
			log.Debug("backupCmd done")

			// Wait for compression to finish
			// If there is still data in the output pipe it can be lost!
			err = compressCmd.Wait()
			if err != nil {
				log.Fatal("compression failed after startup, ", err)
			}
			log.Debug("compressCmd done")

			// If encryption is used wait for it to finish
			if encrypt {
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
		writeStreamToFile(input, filename)
	case "s3":
		writeStreamToS3(input, filename)
	default:
		log.Fatal(backupTo, " no valid value for backupTo")
	}
}

// writeStreamToFile handles a stream and writes it to a local file
func writeStreamToFile(input *io.Reader, backupName string) {
	backupPath := viper.GetString("archivedir") + "/basebackup/" + backupName

	file, err := os.OpenFile(backupPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		log.Fatal("Can not create output file, ", err)
	}
	defer file.Close()

	log.Debug("Start writing to file")
	written, err := io.Copy(file, *input)
	if err != nil {
		log.Fatalf("writeStreamToFile: Error while writing to %s, written %d, error: %v", backupPath, written, err)
	}

	log.Infof("%d bytes were written, waiting for file.Sync()", written)
	file.Sync()
}

// writeStreamToS3 handles a stream and writes it to S3 storage
func writeStreamToS3(input *io.Reader, backupName string) {
	bucket := viper.GetString("s3_bucket_backup")
	location := viper.GetString("s3_location")
	encrypt := viper.GetBool("encrypt")
	contentType := "pgBasebackup"

	// Set contentType for encryption
	if encrypt {
		contentType = "pgp"
	}

	// Initialize minio client object.
	minioClient := getS3Connection()

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		log.Debugf("Bucket already exists, we are using it: %s", bucket)
	} else {
		// Try to create bucket
		err = minioClient.MakeBucket(bucket, location)
		if err != nil {
			log.Debug("minioClient.MakeBucket(bucket, location) failed")
			log.Fatal(err)
		}
		log.Infof("Bucket %s created.", bucket)
	}

	log.Debug("Put backup into bucket")
	n, err := minioClient.PutObject(bucket, backupName, *input, contentType)
	if err != nil {
		log.Debug("minioClient.PutObject(", bucket, ", ", backupName, ", *input,", contentType, ") failed")
		log.Fatal(err)
		return
	}
	log.Infof("Written %d bytes to %s in bucket %s.", n, backupName, bucket)
}

func init() {
	RootCmd.AddCommand(basebackupCmd)
	RootCmd.PersistentFlags().Bool("standalone", true, "Include WAL files in backup so it can be used without WAL archive. If set to false all needed WAL files need to be available via the Archive! If set true the archive is still needed for 'point in time recovery'!")
	// Bind flags to viper
	viper.BindPFlag("standalone", RootCmd.PersistentFlags().Lookup("standalone"))
}
