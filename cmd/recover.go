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
	"errors"
	"os"
	"os/exec"
	"time"

	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	log "github.com/Sirupsen/logrus"
)

// recoverCmd represents the recover command
var recoverCmd = &cobra.Command{
	Use:   "recover WAL_FILE RECOVER_TO",
	Short: "Recovers a given WAL file",
	Long: `This command recovers a given WAL file.
	Example: archive_command = "` + myName + ` recover %f %p"
	
It is intended to use as an restore_command in the recovery.conf.
	Example: restore_command = '` + myName + ` recover %f %p'`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			log.Fatal("Not enough arguments")
		}

		walName := args[0]
		walTarget := args[1]

		err := recoverWal(walTarget, walName)
		if err != nil {
			log.Fatal("recover failed ", err)
		}
		elapsed := time.Since(startTime)
		log.Info("Recovered WAL file in ", elapsed)
	},
}

func init() {
	RootCmd.AddCommand(recoverCmd)
}

// recoverWal recovers a WAL file with the configured method
func recoverWal(walTarget string, walName string) (err error) {
	archiveTo := viper.GetString("archive_to")

	switch archiveTo {
	case "file":
		return recoverWithZstdCommand(walTarget, walName)
	case "s3":
		return recoverFromS3(walTarget, walName)
	default:
		log.Fatal(archiveTo, " no valid value for archiveTo")
	}
	return errors.New("This should never be reached")
}

// recoverWithZstdCommand uses the shell command lz4 to recover WAL files
func recoverWithZstdCommand(walTarget string, walName string) (err error) {
	walSource := viper.GetString("archivedir") + "/wal/" + walName + ".zst"
	log.Debug("recoverWithZstdCommand, walTarget: ", walTarget, ", walName: ", walName, ", walSource: ", walSource)

	// Check if WAL file is already recovered
	if _, err := os.Stat(walTarget); err == nil {
		err := errors.New("WAL file is already recovered in : " + walTarget)
		return err
	}

	recoverCmd := exec.Command(cmdZstd, "-d", walSource, "-o", walTarget)
	err = recoverCmd.Run()
	return err
}

// recoverFromS3 recover from a S3 compatible object store
func recoverFromS3(walTarget string, walName string) (err error) {
	bucket := viper.GetString("s3_bucket_wal")
	walSource := walName + ".zst"

	// Initialize minio client object.
	minioClient := getS3Connection()

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Fatal(err)
	}
	if !exists {
		log.Fatal("Bucket to recover from does not exists")
	}

	walObject, err := minioClient.GetObject(bucket, walSource)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer walObject.Close()

	// Test if the object is accessible
	stat, err := walObject.Stat()
	if err != nil {
		log.Fatal(err)
	}
	if stat.Size <= 0 {
		log.Fatal("WAL object has size <= 0")
	}

	// command to inflate the data stream
	inflateCmd := exec.Command(cmdZstd, "-d", "-o", walTarget)

	// Watch output on stderror
	inflateStderror, err := inflateCmd.StderrPipe()
	check(err)
	go pkg.WatchOutput(inflateStderror, log.Info)

	// Assign walObject as Stdin for the inflate command
	inflateCmd.Stdin = walObject

	// Start WAL inflation
	if err := inflateCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Debug("Inflation started")

	// If there is still data in the output pipe it can be lost!
	err = inflateCmd.Wait()
	if err != nil {
		log.Fatal("inflation failed after startup, ", err)
	}
	return err
}
