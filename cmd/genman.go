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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/util"

	log "github.com/Sirupsen/logrus"
)

// genmanCmd represents the genman command
var (
	manDir    string
	genmanCmd = &cobra.Command{
		Use:   "genman",
		Short: "Generate man page for pgGlaskugel",
		Long:  `Generate man page for pgGlaskugel.`,
		Run: func(cmd *cobra.Command, args []string) {
			header := &doc.GenManHeader{
				Section: "1",
				Manual:  "pgGlaskugel Manual",
				Source:  fmt.Sprintf("pgGlaskugel %s", Version),
			}
			log.Debug("manDir: ", manDir)
			cmd.Root().DisableAutoGenTag = true
			doc.GenManTree(cmd.Root(), header, manDir)
		},
	}
)

func init() {
	pidfile := viper.GetString("pidpath")
	if err := util.CheckPid(pidfile); err != nil {
		log.Error(err)
	} else {
		if err := util.WritePidFile(pidfile); err != nil {
			log.Error(err)
		} else {
			defer util.DeletePidFile(pidfile)
			RootCmd.AddCommand(genmanCmd)
			genmanCmd.Flags().StringVar(&manDir, "man-dir", "docs/man/", "Directory to put the manpage in")
		}
	}

}
