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
	"io"
	"os/exec"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	log "github.com/Sirupsen/logrus"
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	util "github.com/xxorde/pgglaskugel/util"
)

// fetchCmd represents the recover command
var fetchCmd = &cobra.Command{
	Use:   "fetch <WAL_FILE> <FETCH_TO>",
	Short: "Fetches a given WAL file",
	Long: `This command fetches a given WAL file.
	Example: archive_command = "` + myName + ` fetch %f %p"
	
It is intended to use as an restore_command in the recovery.conf.
	Example: restore_command = '` + myName + ` fetch %f %p'`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			log.Fatal("Not enough arguments")
		}

		walName := args[0]
		walTarget := args[1]

		err := fetchWal(walTarget, walName)
		if err != nil {
			log.Fatal("fetch failed ", err)
		}
		elapsed := time.Since(startTime)
		log.Info("Fetched WAL file in ", elapsed)
	},
}

func init() {
	RootCmd.AddCommand(fetchCmd)
}

// fetchWal recovers a WAL file with the configured method
func fetchWal(walTarget string, walName string) (err error) {
	archiveTo := viper.GetString("archive_to")

	switch archiveTo {
	case "file":
		return fetchFromFile(walTarget, walName)
	case "s3":
		return fetchFromS3(walTarget, walName)
	default:
		log.Fatal(archiveTo, " no valid value for archiveTo")
	}
	return errors.New("This should never be reached")
}

// fetchFromFile uses the shell command zstd to recover WAL files
func fetchFromFile(walTarget string, walName string) (err error) {
	walSource := viper.GetString("archivedir") + "/wal/" + walName + ".zst"
	log.Debug("fetchFromFile, walTarget: ", walTarget, ", walName: ", walName, ", walSource: ", walSource)
	encrypt := viper.GetBool("encrypt")

	// If encryption is not used the restore is easy
	if encrypt == false {
		fetchCmd := exec.Command(cmdZstd, "-d", walSource, "-o", walTarget)
		err = fetchCmd.Run()
		return err
	}

	// If we reach this code path encryption is turned on
	// Encryption is used so we have to decrypt
	log.Debug("WAL file will be decrypted")

	// Read and decrypt the compressed data
	gpgCmd := exec.Command(cmdGpg, "--decrypt", "-o", "-", walSource)
	// Set the decryption output as input for inflation
	var gpgStout io.ReadCloser
	gpgStout, err = gpgCmd.StdoutPipe()
	if err != nil {
		log.Fatal("Can not attach pipe to gpg process, ", err)
	}

	// Watch output on stderror
	gpgStderror, err := gpgCmd.StderrPipe()
	ec.Check(err)
	go util.WatchOutput(gpgStderror, log.Info, nil)

	// Start decryption
	if err := gpgCmd.Start(); err != nil {
		log.Fatal("gpg failed on startup, ", err)
	}
	log.Debug("gpg started")

	// command to inflate the data stream
	inflateCmd := exec.Command(cmdZstd, "-d", "-o", walTarget)

	// Watch output on stderror
	inflateDone := make(chan struct{}) // Channel to wait for WatchOutput

	inflateStderror, err := inflateCmd.StderrPipe()
	ec.Check(err)
	go util.WatchOutput(inflateStderror, log.Info, inflateDone)

	// Assign inflationInput as Stdin for the inflate command
	inflateCmd.Stdin = gpgStout

	// Start WAL inflation
	if err := inflateCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Debug("Inflation started")

	// Wait for watch goroutine before Cmd.Wait(), race condition!
	<-inflateDone

	// If there is still data in the output pipe it can be lost!
	err = inflateCmd.Wait()
	ec.CheckCustom(err, "Inflation failed after startup")

	return err
}

// fetchFromS3 recover from a S3 compatible object store
func fetchFromS3(walTarget string, walName string) (err error) {
	log.Debug("fetchFromS3 walTarget: ", walTarget, " walName: ", walName)
	bucket := viper.GetString("s3_bucket_wal")
	walSource := walName + ".zst"
	encrypt := viper.GetBool("encrypt")

	// Initialize minio client object.
	minioClient := getS3Connection()

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Error("Can not test for S3 bucket")
		log.Fatal(err)
	}
	if !exists {
		log.Fatal("Bucket to fetch from does not exists")
	}

	walObject, err := minioClient.GetObject(bucket, walSource)
	if err != nil {
		log.Error("Can not get WAL file from S3")
		log.Fatal(err)
	}
	defer walObject.Close()

	// Test if the object is accessible
	stat, err := walObject.Stat()
	if err != nil {
		log.Error("Can not get stats for WAL file from S3, does WAL file exists?")
		log.Fatal(err)
	}
	if stat.Size <= 0 {
		log.Fatal("WAL object has size <= 0")
	}

	var gpgStout io.ReadCloser
	if encrypt || stat.ContentType == "pgp" {
		log.Debug("content type: ", stat.ContentType)
		// We need to decrypt the wal file

		// Decrypt the compressed data
		gpgCmd := exec.Command(cmdGpg, "--decrypt", "-o", "-")
		// Set the decryption output as input for inflation
		gpgStout, err = gpgCmd.StdoutPipe()
		if err != nil {
			log.Fatal("Can not attach pipe to gpg process, ", err)
		}
		// Attach output of WAL to stdin
		gpgCmd.Stdin = walObject
		// Watch output on stderror
		gpgStderror, err := gpgCmd.StderrPipe()
		ec.Check(err)
		go util.WatchOutput(gpgStderror, log.Info, nil)

		// Start decryption
		if err := gpgCmd.Start(); err != nil {
			log.Fatal("gpg failed on startup, ", err)
		}
		log.Debug("gpg started")
	}

	// command to inflate the data stream
	inflateCmd := exec.Command(cmdZstd, "-d", "-o", walTarget)

	// Watch output on stderror
	inflateDone := make(chan struct{}) // Channel to wait for WatchOutput
	inflateStderror, err := inflateCmd.StderrPipe()
	ec.Check(err)
	go util.WatchOutput(inflateStderror, log.Info, inflateDone)

	// Assign inflationInput as Stdin for the inflate command
	if gpgStout != nil {
		// If gpgStout is defined use it as input
		inflateCmd.Stdin = gpgStout
	} else {
		// If gpgStout is not defined use raw WAL
		inflateCmd.Stdin = walObject
	}

	// Start WAL inflation
	if err := inflateCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Debug("Inflation started")

	// Wait for watch goroutine before Cmd.Wait(), race condition!
	<-inflateDone

	// If there is still data in the output pipe it can be lost!
	err = inflateCmd.Wait()
	ec.CheckCustom(err, "Inflation failed after startup, ")
	return err
}
