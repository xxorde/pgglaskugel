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
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	storage "github.com/xxorde/pgglaskugel/storage"
	wal "github.com/xxorde/pgglaskugel/wal"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// archiveCmd represents the archive command
	archiveCmd = &cobra.Command{
		Use:   "archive WAL_FILE...",
		Short: "Archives given WAL file(s)",
		Long: `This command archives given WAL file(s). This command can be used as an archive_command. The command to recover is "recover". 
	Example: archive_command = "` + myName + ` archive %p"`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				log.Fatal("No WAL file was defined!")
			}

			// Counter for WAL files
			count := 0

			// WaitGroup for workers
			var wg sync.WaitGroup

			// Iterate over every WAL file
			for _, walSource := range args {
				walName := filepath.Base(walSource)

				f, err := os.Open(walSource)
				if err != nil {
					log.Error("Can not open WAL file")
					log.Fatal(err)
				}

				walReader := io.ReadCloser(f)

				// Add one worker to our waiting group (for waiting later)
				wg.Add(1)

				// Start worker
				go compressEncryptStream(&walReader, walName, storeWalStream, &wg)

				count++
			}

			// Wait for workers to finish
			//(WAIT FOR THE WORKER FIRST OR WE CAN LOOSE DATA)
			wg.Wait()

			elapsed := time.Since(startTime)
			log.Info("Archived ", count, " WAL file(s) in ", elapsed)
		},
	}
)

func testWalSource(walSource string) (err error) {
	// Get size of backup
	file, err := os.Open(walSource)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return err
	}

	if fi.Size() < wal.MinArchiveSize {
		return errors.New("Input file is too small")
	}

	if fi.Size() > wal.MaxWalSize {
		return errors.New("Input file is too big")
	}

	return nil
}

// storeWalStream takes a stream and persists it with the configured method
func storeWalStream(input *io.Reader, name string) {
	vipermap := viper.AllSettings
	storage.WriteStream(vipermap, input, name, "archive")
}

func init() {
	RootCmd.AddCommand(archiveCmd)
}
