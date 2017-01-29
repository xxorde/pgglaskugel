package util_test

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xxorde/pgglaskugel/util"

	"golang.org/x/crypto/openpgp/packet"
)

var (
	keyBits        int
	keyOutputDir   = "../test/tmp/"
	keyPrefix      string
	keyPrivateFile string
	keyPublicFile  string

	privKey *packet.PrivateKey
	pubKey  *packet.PublicKey

	plainFile = "../test/text/pg1661.txt"
)

func TestKeyGen(t *testing.T) {
	keyBits = 4096
	now := time.Now()
	keyPrefix = "testkey-" + now.Format("2006-01-02T15:04:05")
	keyPrivateFile = filepath.Join(keyOutputDir, keyPrefix+".privkey")
	keyPublicFile = filepath.Join(keyOutputDir, keyPrefix+".pubkey")

	key := util.GenerateKeys(keyBits, keyOutputDir, keyPrefix, keyPrivateFile, keyPublicFile, rand.Reader)
	util.WritePrivateKey(keyPrivateFile, key)
	util.WritePublicKey(keyPublicFile, key)
}

func TestKeyRead(t *testing.T) {
	privKey = util.ReadPrivateKey(keyPrivateFile)
	t.Log(privKey)
	pubKey = util.ReadPublicKey(keyPublicFile)
	t.Log(pubKey)
}

func TestCrypt(t *testing.T) {
	// Get a io.Reader for the plaintext
	plainFileReader, err := os.Open(plainFile)
	if err != nil {
		t.Fail()
	}

	// Create buffer, io.Reader, io.Writer for the plain text
	var plain bytes.Buffer
	plainWriter := bufio.NewWriter(&plain)
	plainReader := bufio.NewReader(&plain)

	// Copy contet of plain text file in buffer plain
	io.Copy(plainWriter, plainFileReader)

	// Create buffer, io.Reader, io.Writer for the cypher text
	var cypher bytes.Buffer
	cypherWriter := bufio.NewWriter(&cypher)
	cypherReader := bufio.NewReader(&cypher)

	// Encrypt the plain text
	encryptWritten := util.Encrypt(plainReader, cypherWriter, privKey, pubKey)
	t.Log("Encrypt, bytes written: ", encryptWritten)

	// Create buffer, io.Reader, io.Writer for the decrypted text
	var decrypt bytes.Buffer
	decryptWriter := bufio.NewWriter(&decrypt)
	decryptReader := bufio.NewReader(&decrypt)

	// Decrypt the cypher text
	decryptWritten := util.Decrypt(cypherReader, decryptWriter, privKey, pubKey)
	t.Log("Decrypt, bytes written: ", decryptWritten)

	bytes.Compare(plain., decrypt)

	t.Log()
}
