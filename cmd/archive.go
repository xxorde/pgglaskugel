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

	log "github.com/Sirupsen/logrus"

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
				err = archiveWithZstdCommand(walSource, walName)
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

	if fi.Size() < 100 {
		return errors.New("Input file to small!")
	}

	if fi.Size() > 16777216 {
		return errors.New("Input file to big!")
	}

	return nil
}

// archiveToFile archives to a local file (implemented in Go)
func archiveToFile(walSource string, walName string) (err error) {
	// TODO...
	return err
}

// archiveToS3 archives to a S3 compatible object store
func archiveToS3(walSource string, walName string) (err error) {
	// TODO...
	return err
}

// archiveWithLz4Command uses the shell command lz4 to archive WAL files
func archiveWithZstdCommand(walSource string, walName string) (err error) {
	walTarget := viper.GetString("archivedir") + "/wal/" + walName + ".zstd"
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
