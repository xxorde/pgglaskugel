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

			backupTime := startTime.Format(pkg.BackupTimeFormat)
			backupName := "bb@" + backupTime
			backupPath := viper.GetString("archivedir") + "/basebackup/" + backupName + ".zst"

			backupCmd := exec.Command("pg_basebackup", "-Ft", "--verbose", "-l", backupName, "--checkpoint", "fast", "-D", "-")
			//backupCmd.Env = []string{"PGOPTIONS='--client-min-messages=WARNING'"}

			// attach pipe to the command
			backupStdout, err := backupCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Watch stderror
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

			// Watch stderror
			compressStderror, err := compressCmd.StderrPipe()
			check(err)
			go pkg.WatchOutput(compressStderror, log.Info)

			// Pipe the backup in the compression
			compressCmd.Stdin = backupStdout

			// Add one worker to our waiting group (for waiting later)
			wg.Add(1)

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
			go writeStreamToFile(compressStdout, backupPath, &wg)

			// Wait for workers to finish
			//(WAIT FIRST FOR THE WORKER OR WE CAN LOOSE DATA)
			wg.Wait()

			// Wait for backup to finish
			err = backupCmd.Wait()
			if err != nil {
				log.Fatal("pg_basebackup failed after startup, ", err)
			}
			log.Debug("backupCmd done")

			// Wait for compression to finish
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

// writeStreamToFile handles a stream and writes it to a local file
func writeStreamToFile(input io.ReadCloser, filename string, wg *sync.WaitGroup) {
	// Tell the waiting group this process is done when function ends
	defer wg.Done()

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		log.Fatal("Can not create output file, ", err)
	}
	defer file.Close()

	log.Debug("Start writing to file")
	written, err := io.Copy(file, input)
	if err != nil {
		log.Fatalf("writeStreamToFile: Error while writing to %s, written %d, error: %v", filename, written, err)
	}

	log.Infof("%d bytes were written, waiting for file.Sync()", written)
	file.Sync()
}

// writeStreamToS3 handles a stream and writes it to S3 storage
func writeStreamToS3(input io.ReadCloser, filename string, wg *sync.WaitGroup) {
	// Tell the waiting group this process is done when function ends
	defer wg.Done()

	endpoint := "play.minio.io:9000"
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	ssl := true
	bucket := "pgGlaskugel"
	location := "us-east-1"

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, ssl)
	if err != nil {
		log.Fatal(err)
	}

	// Creates bucket with name mybucket.
	err = minioClient.MakeBucket(bucket, location)
	if err != nil {
		log.Warning(err)
		return
	}
	log.Info("Successfully created mybucket.")

	// Upload an object 'myobject.txt' with contents from '/home/joe/myfilename.txt'
	n, err := minioClient.PutObject(bucket,filename,input,"application/octet-stream")
		if err != nil {
		log.Fatal(err)
		return
	}

	log.Info("Written to bucket: ",n)
}

func init() {
	RootCmd.AddCommand(basebackupCmd)
}
