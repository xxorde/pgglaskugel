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
	"github.com/siddontang/go/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// restoreCmd represents the restore command
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore an existing backup to a given location",
	Long:  `Restore a given backup to a given location.`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug("restore called")

	},
}

func init() {
	RootCmd.AddCommand(restoreCmd)
	restoreCmd.PersistentFlags().StringP("backup", "S", "myBackup@2016-11-04T21:52:57", "The backup tor restore")
	restoreCmd.PersistentFlags().StringP("destination", "D", "/var/lib/postgres/pgGlaskugel-restore", "The destination to restored to")
	restoreCmd.PersistentFlags().Bool("force-restore", false, "Force the deletion of existing data (danger zone)!")

	// Bind flags to viper
	viper.BindPFlag("backup", restoreCmd.PersistentFlags().Lookup("backup"))
	viper.BindPFlag("destination", restoreCmd.PersistentFlags().Lookup("destination"))
	viper.BindPFlag("force-restore", restoreCmd.PersistentFlags().Lookup("force-restore"))
}
