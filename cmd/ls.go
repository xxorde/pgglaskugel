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
	"io/ioutil"

	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	"github.com/spf13/cobra"

	log "github.com/Sirupsen/logrus"
)

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "Shows existing backups",
	Long:  `Shows you all backups already made with meta information.`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here
		backupDir := archiveDir + "/basebackup"
		backupPath, err := getAllBackups(backupDir)
		check(err)

		var backups pkg.Backups

		log.Info("Backups in: ", backupDir)
		for _, backup := range backupPath {
			log.Info(backup)
			err = backups.Add(backup)
			if err != nil {
				log.Warn(err)
			}
		}

		log.Info(backups)
	},
}

func getAllBackups(backupDir string) (backups []string, err error) {
	log.Debug("getAllBackups in ", backupDir)
	backups = make([]string, 0)
	files, _ := ioutil.ReadDir(backupDir)
	for _, f := range files {
		backups = append(backups, backupDir+"/"+f.Name())
	}
	return backups, nil
}

func init() {
	RootCmd.AddCommand(lsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// lsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// lsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
