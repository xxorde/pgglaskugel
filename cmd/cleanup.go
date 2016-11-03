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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gogs.xxor.de/xxorde/pgGlaskugel/pkg"

	log "github.com/Sirupsen/logrus"
)

// cleanupCmd represents the cleanup command
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Deletes backups and WAL files enforcing an retention policy",
	Long: `Enforces your retention policy by deleting backups and WAL files.
	Use with care.`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here
		fmt.Println("cleanup called")

		backupDir := archiveDir + "/basebackup"
		var backups pkg.Backups
		backups.GetBackupsInDir(backupDir)

		keep, discard := backups.SeparateBackupsByAge(3)
		log.Info("Keep the following backups:", keep.String())
		log.Info("DELETE the following backups: ", discard.String())
	},
}

func init() {
	RootCmd.AddCommand(cleanupCmd)
	setupCmd.PersistentFlags().Int("retain", -1, "How many backups should we keep?")

	// Bind flags to viper
	viper.BindPFlag("retain", setupCmd.PersistentFlags().Lookup("retain"))
}
