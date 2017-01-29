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
	"crypto/rand"
	"path/filepath"

	log "github.com/Sirupsen/logrus"
	"github.com/xxorde/pgglaskugel/util"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// keygenCmd represents the keygen command
var keygenCmd = &cobra.Command{
	Use:   "keygen",
	Short: "Generate RSA keys for use with pgGlaskugel",
	Long: `This command can generate keys for use with pgGlaskugel. Consider to use pre-generated keys with other tools like GnuPG.
	WARNING: Who ever can access the private key can decrypt the backups!
	WARNING: If the private key is lost, all encrypted backups are LOST too!`,
	Run: func(cmd *cobra.Command, args []string) {
		keyBits := viper.GetInt("keyBits")
		keyOutputDir := viper.GetString("keyOutputDir")
		keyPrefix := viper.GetString("keyPrefix")
		keyPrivateFile := filepath.Join(keyOutputDir, keyPrefix+".privkey")
		keyPublicFile := filepath.Join(keyOutputDir, keyPrefix+".pubkey")

		log.Info("Generate key pair")
		log.Info("keyBits: ", keyBits)
		log.Info("keyOutputDir: ", keyOutputDir)
		log.Info("keyPrefix: ", keyPrefix)
		log.Info("keyPrivateFile: ", keyPrivateFile)
		log.Info("keyPublicFile: ", keyPublicFile)

		key := util.GenerateKeys(keyBits, keyOutputDir, keyPrefix, keyPrivateFile, keyPublicFile, rand.Reader)
		util.WritePrivateKey(keyPrivateFile, key)
		util.WritePublicKey(keyPublicFile, key)
		printDone()
	},
}

func init() {
	RootCmd.AddCommand(keygenCmd)

	keygenCmd.PersistentFlags().String("keyPrefix", "pgGlaskugel", "The prefix for public and private key")
	keygenCmd.PersistentFlags().String("keyOutputDir", "./", "The prefix for public and private key")
	keygenCmd.PersistentFlags().Int("keyBits", 4096, "Key size in bits, sane values are 2048 or 4096")

	// Bind flags to viper
	viper.BindPFlag("keyPrefix", keygenCmd.PersistentFlags().Lookup("keyPrefix"))
	viper.BindPFlag("keyOutputDir", keygenCmd.PersistentFlags().Lookup("keyOutputDir"))
	viper.BindPFlag("keyBits", keygenCmd.PersistentFlags().Lookup("keyBits"))
}
