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
	"os"

	log "github.com/Sirupsen/logrus"
	lz4 "github.com/bkaradzic/go-lz4"

	"github.com/spf13/cobra"
)

var (
	// Variables for flags
	decompress bool

	// lz4Cmd represents the lz4 command
	lz4Cmd = &cobra.Command{
		Use:   "lz4 [-d|--decompress] <input> <output>",
		Short: "Compresses and decompresses using lz4",
		Long: `This command can be used to compress and uncompress using lz4. It is provided in case there is no other tool on your system. Performance is not perfect. Other tools should be preferred!
Example:
  Compress:
    ` + myName + ` dump.sql dump.sql.lz4
  Decompress:
    ` + myName + ` -d dump.sql.lz4 dump.sql
`,
		Run: func(cmd *cobra.Command, args []string) {

			var data []byte

			if len(args) < 2 {
				log.Fatal("Not enough arguments")
			}

			input, err := os.OpenFile(args[0], os.O_RDONLY, 0644)
			if err != nil {
				log.Fatalf("Failed to open input file %s\n", args[0])
			}
			defer input.Close()

			// Decompress the input file
			if decompress {
				data, _ = ioutil.ReadAll(input)
				data, err = lz4.Decode(nil, data)
				if err != nil {
					log.Error("Failed to decode:", err)
					return
				}
			} else { // Compress the input file
				data, _ = ioutil.ReadAll(input)
				data, err = lz4.Encode(nil, data)
				if err != nil {
					log.Error("Failed to encode:", err)
					return
				}
			}

			err = ioutil.WriteFile(args[1], data, 0644)
			if err != nil {
				log.Fatalf("Failed to open output file %s\n", args[1])
			}

		},
	}
)

func init() {
	RootCmd.AddCommand(lz4Cmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// lz4Cmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// lz4Cmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	lz4Cmd.Flags().BoolVarP(&decompress, "decompress", "d", false, "Decompress the input (instead of compress)")
}
