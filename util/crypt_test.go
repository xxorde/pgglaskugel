package util_test

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"path/filepath"
	"strings"
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

	plainFile     = "../test/text/pg1661.txt"
	plainFileSize = 594933
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
	if privKey == nil {
		t.Fail()
	}

	pubKey = util.ReadPublicKey(keyPublicFile)
	if pubKey == nil {
		t.Fail()
	}
}

func TestCrypt(t *testing.T) {
	// Read in plain text from file
	plainText, err := ioutil.ReadFile(plainFile)
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	if len(plainText) != plainFileSize {
		t.Log("Size of plaintext does not match preset plainFileSize plainText:", plainText, "plainFileSize:", plainFileSize)
		t.Fail()
	}

	// Create buffer for plain text and fill with text
	plainBuffer := bytes.NewBuffer(plainText)

	// Create buffer for cypher text
	cypherBuffer := new(bytes.Buffer)

	// Encrypt the plain text
	encryptWritten := util.Encrypt(plainBuffer, cypherBuffer, privKey, pubKey)
	t.Log("Encrypt, bytes written: ", encryptWritten)

	// Create for the decrypted text
	decryptBuffer := new(bytes.Buffer)

	// Decrypt the cypher text
	decryptWritten := util.Decrypt(cypherBuffer, decryptBuffer, privKey, pubKey)
	t.Log("Decrypt, bytes written: ", decryptWritten)

	// Convert plaintext and decrypted text to string and compare them
	delta := strings.Compare(string(plainText), decryptBuffer.String())
	t.Log("plain text size: ", len(plainText))
	t.Log("decrypted text size: ", decryptBuffer.Len())
	t.Log("The delta between plaintext and decrypted text is: ", delta)
	if delta != 0 {
		t.Log("Plaintext and decrypted text do not match")
		t.Fail()
	}
	if decryptBuffer.Len() != plainFileSize {
		t.Log("Size of decrypted text does not match preset plainFileSize, decryptBuffer.Len():", decryptBuffer.Len(), "plainFileSize:", plainFileSize)
		t.Fail()
	}
}
