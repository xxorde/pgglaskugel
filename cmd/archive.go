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
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	log "github.com/Sirupsen/logrus"
	minio "github.com/minio/minio-go"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// archiveCmd represents the archive command
	archiveCmd = &cobra.Command{
		Use:   "archive",
		Short: "Archive a WAL file",
		Long: `This command archives a given WAL file. This command can be used as an archive_command.
	Example: archive_command = "` + myName + ` archive %p"`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				log.Fatal("No WAL file was defined!")
			}

			count := 0
			for _, walSource := range args {
				err := testWalSource(walSource)
				check(err)
				walName := filepath.Base(walSource)
				err = archiveWal(walSource, walName)
				if err != nil {
					log.Fatal("archive failed ", err)
				}
				count++
			}
			elapsed := time.Since(startTime)
			log.Info("Archived ", count, " WAL file(s) in ", elapsed)
		},
	}
)

func testWalSource(walSource string) (err error) {
	// Get size of backup
	file, err := os.Open(walSource)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return err
	}

	if fi.Size() < minArchiveSize {
		return errors.New("Input file to small!")
	}

	if fi.Size() > maxWalSize {
		return errors.New("Input file to big!")
	}

	return nil
}

// archiveWal archives a WAL file with the configured method
func archiveWal(walSource string, walName string) (err error) {
	archiveTo := viper.GetString("archive_to")

	switch archiveTo {
	case "file":
		return archiveWithZstdCommand(walSource, walName)
	case "s3":
		return archiveToS3(walSource, walName)
	default:
		log.Fatal(archiveTo, " no valid value for archiveTo")
	}
	return errors.New("This should never ben reached")
}

// archiveToS3 archives to a S3 compatible object store
func archiveToS3(walSource string, walName string) (err error) {
	endpoint := viper.GetString("s3_endpoint")
	accessKeyID := viper.GetString("s3_access_key")
	secretAccessKey := viper.GetString("s3_secret_key")
	ssl := viper.GetBool("s3_ssl")
	bucket := viper.GetString("s3_bucket_wal")
	location := viper.GetString("s3_location")
	walTarget := walName + ".zst"

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, ssl)
	if err != nil {
		log.Fatal(err)
	}
	minioClient.SetAppInfo(myName, myVersion)
	log.Debugf("%v", minioClient)

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		log.Infof("Bucket already exists, we are using it: %s", bucket)
	} else {
		// Try to create bucket
		err = minioClient.MakeBucket(bucket, location)
		if err != nil {
			log.Fatal(err)
		}
		log.Infof("Bucket %s created.", bucket)
	}

	// This command is used to take the backup and compress it
	compressCmd := exec.Command("/usr/bin/zstd", "--stdout", walSource)

	// attach pipe to the command
	compressStdout, err := compressCmd.StdoutPipe()
	if err != nil {
		log.Fatal("Can not attach pipe to backup process, ", err)
	}

	// Watch output on stderror
	compressStderror, err := compressCmd.StderrPipe()
	check(err)
	go pkg.WatchOutput(compressStderror, log.Info)

	// Start backup and compression
	if err := compressCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Debug("Compression started")

	n, err := minioClient.PutObject(bucket, walTarget, compressStdout, "")
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Infof("Written %d bytes to %s in bucket %s.", n, walTarget, bucket)

	// If there is still data in the output pipe it can be lost!
	err = compressCmd.Wait()
	if err != nil {
		log.Fatal("compression failed after startup, ", err)
	}
	return err
}

// archiveWithLz4Command uses the shell command lz4 to archive WAL files
func archiveWithZstdCommand(walSource string, walName string) (err error) {
	walTarget := viper.GetString("archivedir") + "/wal/" + walName + ".zst"
	log.Debug("archiveWithLz4Command, walSource: ", walSource, ", walName: ", walName, ", walTarget: ", walTarget)

	// Check if WAL file is already in archive
	if _, err := os.Stat(walTarget); err == nil {
		err := errors.New("WAL file is already in archive: " + walTarget)
		return err
	}

	archiveCmd := exec.Command("/usr/bin/zstd", walSource, "-o", walTarget)
	err = archiveCmd.Run()
	return err
}

func init() {
	RootCmd.AddCommand(archiveCmd)
}
