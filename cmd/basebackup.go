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

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	// Number of bytes to read per iteration
	nBytes = 64
)

// basebackupCmd represents the basebackup command
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
			//backupCmd := exec.Command("/usr/bin/pg_basebackup", "-D", "-", "-Ft")
			backupCmd := exec.Command("cat", "pg1661.txt")
			//backupCmd := exec.Command("uptime")

			stdout, err := backupCmd.StdoutPipe()
			if err != nil {
				log.Fatal("Can not attach pipe to backup process, ", err)
			}

			// Start worker
			wg.Add(1)
			go handleBackup(stdout)

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
		},
	}
)

func handleBackup(stdoutPipe io.ReadCloser) {
	log.Debug("Run handleBackup")
	defer wg.Done()
	writtenSum := 0
	in := bufio.NewScanner(stdoutPipe)
	file, err := os.Create("t1.out")
	if err != nil {
		log.Fatal("Can not create output file, ", err)
	}
	defer file.Close()

	// Use custom split function to only scan nBytes
	in.Split(bufio.ScanLines)

	for in.Scan() {
		log.Debug("Reading chunk ...")
		written, err := file.Write(in.Bytes())
		if err != nil {
			log.Fatal("Failed to open output file: ", err)
		}
		writtenSum += written
		log.Debug(string(in.Bytes()))
		log.Debugf(" ... written %d (total %d)\n", written, writtenSum)
		if err := in.Err(); err != nil {
			log.Fatalf("int error: %s", err)
		}
	}
	if err := in.Err(); err != nil {
		log.Fatalf("handleBackup error: %s", err)
	}
	if err := file.Sync(); err != nil {
		log.Fatal("Error wile sync data, ", err)
	}

	log.Debug("Total written bytes: ", writtenSum)
}

// Split function to use with *Scanner.Scan()
func scanNBytes(data []byte, atEOF bool) (advance int, token []byte, err error) {
	log.Info(len(data))
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if len(data) < nBytes {
		return len(data), data[0:len(data)], nil
	}

	return nBytes, data[0:nBytes], nil
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
