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
	"bytes"
	"os/exec"

	"github.com/spf13/cobra"

	log "github.com/Sirupsen/logrus"
)

// basebackupCmd represents the basebackup command
var (
	baseBackupTools = []string{
		"pg_basebackup",
	}

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
			backupCmd := exec.Command("sleep", "5")

			// define a buffer for output and fill it wirh stdout
			var out bytes.Buffer
			backupCmd.Stdout = &out

			// Start the process (in the background)
			err = backupCmd.Start()
			if err != nil {
				log.Fatal("pg_basebackup failed on startup, ", err)
			}
			log.Info("Backup was started")

			// Wait for backup to finish
			err = backupCmd.Wait()
			if err != nil {
				log.Fatal("pg_basebackup failed after startup, ", err)
			}

			log.Debug("pg_basebackup out: ", out.String())
		},
	}
)

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
