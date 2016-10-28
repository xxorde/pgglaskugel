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

	log "github.com/Sirupsen/logrus"
	"github.com/pierrec/lz4"
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
			backupCmd := exec.Command("/usr/bin/pg_basebackup", "-D", "-", "-Ft", "--checkpoint", "fast")
			//backupCmd := exec.Command("cat", "pg1661.txt")

			// attach pipe to the command
			stdout, err := backupCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Add one worker to our waiting group (for waiting later)
			wg.Add(1)
			// Start worker
			go writeStreamLz4(stdout, viper.GetString("archivedir")+"/basebackup/"+"t1.lz4", &wg)

			// Start the process (in the background)
			if err := backupCmd.Start(); err != nil {
				log.Fatal("pg_basebackup failed on startup, ", err)
			}
			log.Info("Backup was started")

			// Wait for backup to finish
			err = backupCmd.Wait()
			if err != nil {
				log.Fatal("pg_basebackup failed after startup, ", err)
			}

			// Wait for workers to finish
			wg.Wait()
			elapsed := time.Since(startTime)
			log.Info("Backup done in ", elapsed)
		},
	}
)

// handle a stream, compress with lz4 and write to file
func writeStreamLz4(reader io.ReadCloser, filename string, wg *sync.WaitGroup) {
	// Tell the waiting group this process is done when function ends
	defer wg.Done()

	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("Can not create output file, ", err)
	}
	defer file.Close()

	zw := lz4.NewWriter(nil)

	worker := func(in io.Reader, out io.Writer) {
		zw.Reset(out)
		zw.Header = lz4Header
		if _, err := io.Copy(zw, in); err != nil {
			log.Fatalf("Error while compressing input: %v", err)
		}
	}

	log.Debug("Start writing compressed backup now")
	worker(reader, file)

	log.Info("Data is written, waiting for file.Sync()")
	file.Sync()
}

func init() {
	RootCmd.AddCommand(basebackupCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// basebackupCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// basebackupCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
