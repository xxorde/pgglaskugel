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
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/storage"

	log "github.com/Sirupsen/logrus"
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
	vipermap := viper.AllSettings
	return storage.Fetch(vipermap, walTarget, walName)
}
