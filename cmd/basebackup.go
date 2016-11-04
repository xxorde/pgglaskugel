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
	"bufio"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// Number of bytes to read per iteration
	nBytes = 64
)

var (
	baseBackupTools = []string{
		"pg_basebackup",
		"zstd",
	}

	// WaitGroup for workers
	wg sync.WaitGroup

	basebackupCmd = &cobra.Command{
		Use:   "basebackup",
		Short: "Creates a new basebackup from the database",
		Long:  `Creates a new basebackup from the database with the given method.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("Perform basebackup")

			// Check if needed tools are available
			err := testTools(baseBackupTools)
			check(err)

			// Connect to database
			//conString := viper.GetString("connection")
			//			backupCmd := exec.Command("/usr/bin/pg_basebackup", "-d", "'"+conString+"'", "-D", viper.GetString("archivedir")+"/basebackup", "--format", "tar", "--gzip", "--checkpoint", "fast")
			backupCmd := exec.Command("pg_basebackup", "-Ft", "--checkpoint", "fast", "-D", "-")
			//backupCmd.Env = []string{"PGOPTIONS='--client-min-messages=WARNING'"}

			// attach pipe to the command
			backupStdout, err := backupCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Watch stderror
			backupStderror, err := backupCmd.StderrPipe()
			check(err)
			go watchOutput(backupStderror, log.Info)

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
			go watchOutput(compressStderror, log.Info)

			// Pipe the backup in the compression
			compressCmd.Stdin = backupStdout

			backupTime := startTime.Format(pkg.BackupTimeFormat)
			backupName := "bb@" + backupTime + ".zstd"
			backupPath := viper.GetString("archivedir") + "/basebackup/" + backupName

			// Add one worker to our waiting group (for waiting later)
			wg.Add(1)

			// Start worker
			go writeStreamToFile(compressStdout, backupPath, &wg)

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

			// Wait for workers to finish
			wg.Wait()
			elapsed := time.Since(startTime)
			log.Info("Backup done in ", elapsed)
		},
	}
)

// handle a stream and write to file
func writeStreamToFile(input io.ReadCloser, filename string, wg *sync.WaitGroup) {
	// Tell the waiting group this process is done when function ends
	defer wg.Done()

	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("Can not create output file, ", err)
	}
	defer file.Close()

	log.Debug("Start writing to file")
	written, err := io.Copy(file, input)
	if err != nil {
		log.Fatalf("Error while writing to %s, written %d, error: %v", filename, written, err)
	}

	log.Infof("%d bytes were written, waiting for file.Sync()", written)
	file.Sync()
}

func watchOutput(input io.Reader, outputFunc func(args ...interface{})) {
	log.Debug("watchOutput started")
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		outputFunc(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		outputFunc("reading standard input:", err)
	}
	log.Debug("watchOutput end")
}

func init() {
	RootCmd.AddCommand(basebackupCmd)
}
