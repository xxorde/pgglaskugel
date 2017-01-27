package pkg

import (
	"crypto/rsa"
	"io"
	"time"

	ec "github.com/xxorde/pgglaskugel/errorcheck"

	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/armor"
	"golang.org/x/crypto/openpgp/packet"
)

func EncodePrivateKey(out io.Writer, key *rsa.PrivateKey) {
	w, err := armor.Encode(out, openpgp.PrivateKeyType, make(map[string]string))
	ec.CheckFatalCustom(err, "Error creating OpenPGP Armor:")

	pgpKey := packet.NewRSAPrivateKey(time.Now(), key)
	ec.CheckFatalCustom(pgpKey.Serialize(w), "Error serializing private key:")
	ec.CheckFatalCustom(w.Close(), "Error serializing private key:")
}

func EncodePublicKey(out io.Writer, key *rsa.PrivateKey) {
	w, err := armor.Encode(out, openpgp.PublicKeyType, make(map[string]string))
	ec.CheckFatalCustom(err, "Error creating OpenPGP Armor:")

	pgpKey := packet.NewRSAPublicKey(time.Now(), &key.PublicKey)
	ec.CheckFatalCustom(pgpKey.Serialize(w), "Error serializing public key:")
	ec.CheckFatalCustom(w.Close(), "Error serializing public key:")
}
