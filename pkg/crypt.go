package pkg

import (
	"crypto/rsa"
	"errors"
	"io"
	"os"
	"time"

	log "github.com/siddontang/go/log"

	ec "github.com/xxorde/pgglaskugel/errorcheck"

	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/armor"
	"golang.org/x/crypto/openpgp/packet"
)

func GenerateKeys(keyBits int, keyOutputDir string, keyPrefix string,
	keyPrivateFile string, keyPublicFile string, entropy io.Reader) *rsa.PrivateKey {

	key, err := rsa.GenerateKey(entropy, keyBits)
	if err != nil {
		log.Fatal("Error generating RSA key: ", err)
	}
	return key
}

func WritePrivateKey(keyPrivateFile string, key *rsa.PrivateKey) {
	// Create file an open write only, do NOT override existing keys!
	priv, err := os.OpenFile(keyPrivateFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	ec.CheckFatalCustom(err, "Error writing private key file: ")
	defer priv.Close()

	w, err := armor.Encode(priv, openpgp.PrivateKeyType, make(map[string]string))
	ec.CheckFatalCustom(err, "Error creating OpenPGP Armor:")

	pgpKey := packet.NewRSAPrivateKey(time.Now(), key)
	ec.CheckFatalCustom(pgpKey.Serialize(w), "Error serializing private key:")
	ec.CheckFatalCustom(w.Close(), "Error serializing private key:")
}

func WritePublicKey(keyPublicFile string, key *rsa.PrivateKey) {
	// Create file an open write only, do NOT override existing keys!
	pub, err := os.OpenFile(keyPublicFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	ec.CheckFatalCustom(err, "Error writing public key file: ")
	defer pub.Close()

	w, err := armor.Encode(pub, openpgp.PublicKeyType, make(map[string]string))
	ec.CheckFatalCustom(err, "Error creating OpenPGP Armor:")

	pgpKey := packet.NewRSAPublicKey(time.Now(), &key.PublicKey)
	ec.CheckFatalCustom(pgpKey.Serialize(w), "Error serializing public key:")
	ec.CheckFatalCustom(w.Close(), "Error serializing public key:")
}

func ReadPrivateKey(filename string) *packet.PrivateKey {
	in, err := os.Open(filename)
	ec.CheckFatalCustom(err, "Error opening private key:")
	defer in.Close()

	block, err := armor.Decode(in)
	ec.CheckFatalCustom(err, "Error decoding OpenPGP Armor:")

	if block.Type != openpgp.PrivateKeyType {
		ec.CheckFatal(errors.New("Invalid private key file"))
	}

	reader := packet.NewReader(block.Body)
	pkt, err := reader.Next()
	ec.CheckFatalCustom(err, "Error reading private key")

	key, ok := pkt.(*packet.PrivateKey)
	if !ok {
		ec.CheckFatal(errors.New("Invalid private key"))
	}
	return key
}

func ReadPublicKey(filename string) *packet.PublicKey {
	in, err := os.Open(filename)
	ec.CheckFatalCustom(err, "Error opening public key:")
	defer in.Close()

	block, err := armor.Decode(in)
	ec.CheckFatalCustom(err, "Error decoding OpenPGP Armor:")

	if block.Type != openpgp.PublicKeyType {
		ec.CheckFatal(errors.New("Invalid private key file"))
	}

	reader := packet.NewReader(block.Body)
	pkt, err := reader.Next()
	ec.CheckFatalCustom(err, "Error reading private key")

	key, ok := pkt.(*packet.PublicKey)
	if !ok {
		ec.CheckFatal(errors.New("Invalid public key"))
	}
	return key
}
