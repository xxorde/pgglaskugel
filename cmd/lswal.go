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
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
)

// lswalCmd represents the lswal command
var lswalCmd = &cobra.Command{
	Use:   "lswal",
	Short: "Show all WAL files in archive",
	Long: "Show a detailed list of the archived WAL files already backuped",
	Run: func(cmd *cobra.Command, args []string) {
		showWals()
		printDone()
	},
}

func showWals() {
	archive := getMyWals()
	log.Info(archive.String())
}

func init() {
	RootCmd.AddCommand(lswalCmd)
}
