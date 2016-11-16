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
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	log "github.com/Sirupsen/logrus"
	minio "github.com/minio/minio-go"
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
			backupTime := startTime.Format(pkg.BackupTimeFormat)
			backupName := "bb@" + backupTime + ".zst"

			// Command to use pg_basebackup
			// Tar format, set backupName as label, make fast checkpoints, return output on standardout
			backupCmd := exec.Command("pg_basebackup", "-Ft", "-l", backupName, "--checkpoint", "fast", "-D", "-")
			// attach pipe to the command
			backupStdout, err := backupCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Watch output on stderror
			backupStderror, err := backupCmd.StderrPipe()
			check(err)
			go pkg.WatchOutput(backupStderror, log.Info)

			// This command is used to take the backup and compress it
			compressCmd := exec.Command("zstd")

			// attach pipe to the command
			compressStdout, err := compressCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Watch output on stderror
			compressStderror, err := compressCmd.StderrPipe()
			check(err)
			go pkg.WatchOutput(compressStderror, log.Info)

			// Pipe the backup in the compression
			compressCmd.Stdin = backupStdout

			// Start the process (in the background)
			if err := backupCmd.Start(); err != nil {
				log.Fatal("pg_basebackup failed on startup, ", err)
			}
			log.Info("Backup was started")

			// Start backup and compression
			if err := compressCmd.Start(); err != nil {
				log.Fatal("pg_basebackup failed on startup, ", err)
			}
			log.Info("Compression started")

			// Start worker
			// Add one worker to our waiting group (for waiting later)
			wg.Add(1)
			go handleBackupStream(compressStdout, backupName, &wg)

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

			elapsed := time.Since(startTime)
			log.Info("Backup done in ", elapsed)
		},
	}
)

// handleBackupStream takes a stream and persists it with the configured method
func handleBackupStream(input io.ReadCloser, filename string, wg *sync.WaitGroup) {
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
func writeStreamToFile(input io.ReadCloser, backupName string) {
	backupPath := viper.GetString("archivedir") + "/basebackup/" + backupName

	file, err := os.OpenFile(backupPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		log.Fatal("Can not create output file, ", err)
	}
	defer file.Close()

	log.Debug("Start writing to file")
	written, err := io.Copy(file, input)
	if err != nil {
		log.Fatalf("writeStreamToFile: Error while writing to %s, written %d, error: %v", backupPath, written, err)
	}

	log.Infof("%d bytes were written, waiting for file.Sync()", written)
	file.Sync()
}

// writeStreamToS3 handles a stream and writes it to S3 storage
func writeStreamToS3(input io.ReadCloser, backupName string) {
	endpoint := viper.GetString("s3_endpoint")
	accessKeyID := viper.GetString("s3_access_key")
	secretAccessKey := viper.GetString("s3_secret_key")
	ssl := viper.GetBool("s3_ssl")
	bucket := viper.GetString("s3_bucket_backup")
	location := viper.GetString("s3_location")

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, ssl)
	if err != nil {
		log.Fatal(err)
	}
	minioClient.SetAppInfo(myName, myVersion)
	log.Debugf("%v\n", minioClient)

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		log.Infof("Bucket already exists, we are using it: %s\n", bucket)
	} else {
		// Try to create bucket
		err = minioClient.MakeBucket(bucket, location)
		if err != nil {
			log.Fatal(err)
		}
		log.Infof("Bucket %s created.\n", bucket)
	}
	n, err := minioClient.PutObject(bucket, backupName, input, "")
	if err != nil {
		log.Fatal(err)
		return
	}

	log.Infof("Written %d bytes to %s in bucket %s.\n", n, backupName, bucket)
}

func init() {
	RootCmd.AddCommand(basebackupCmd)
}
