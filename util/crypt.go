package util

import (
	"crypto"
	"crypto/rsa"
	_ "crypto/sha256"
	"errors"
	"io"
	"os"
	"time"

	_ "golang.org/x/crypto/ripemd160"

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

func Encrypt(plainIn io.Reader, cypherOut io.Writer, privKey *packet.PrivateKey, pubKey *packet.PublicKey) (written int64) {
	to := CreateEntityFromKeys(privKey, pubKey)

	w, err := armor.Encode(cypherOut, "Message", make(map[string]string))
	ec.CheckFatalCustom(err, "Error creating OpenPGP Armor:")
	defer w.Close()

	plain, err := openpgp.Encrypt(w, []*openpgp.Entity{to}, nil, nil, nil)
	ec.CheckFatalCustom(err, "Error creating entity for encryption")
	defer plain.Close()

	//compressed, err := gzip.NewWriterLevel(plain, gzip.BestCompression)
	//ec.CheckFatalCustom(err, "Invalid compression level")
	//defer compressed.Close()

	written, err = io.Copy(plain, plainIn)
	ec.CheckFatalCustom(err, "Error writing encrypted file")
	log.Debug("written: ", written)

	return written
}

func Decrypt(cypheIn io.Reader, plainOut io.Writer, privKey *packet.PrivateKey, pubKey *packet.PublicKey) (written int64) {
	entity := CreateEntityFromKeys(privKey, pubKey)

	block, err := armor.Decode(cypheIn)
	ec.CheckCustom(err, "Error reading OpenPGP Armor:")

	if block.Type != "Message" {
		ec.CheckCustom(err, "Invalid message type")
	}

	var entityList openpgp.EntityList
	entityList = append(entityList, entity)

	md, err := openpgp.ReadMessage(block.Body, entityList, nil, nil)
	ec.CheckCustom(err, "Error reading message")

	//	compressed, err := gzip.NewReader(md.UnverifiedBody)
	//	ec.CheckCustom(err, "Invalid compression level")
	//	defer compressed.Close()

	written, err = io.Copy(plainOut, md.UnverifiedBody)
	ec.CheckCustom(err, "Error reading encrypted file")
	log.Debug("Decrypted %d bytes", written)

	return written
}

func CreateEntityFromKeys(privKey *packet.PrivateKey, pubKey *packet.PublicKey) *openpgp.Entity {
	log.Debug("CreateEntityFromKeys")
	privBits, err := privKey.BitLength()
	ec.Check(err)

	pubBits, err := pubKey.BitLength()
	ec.Check(err)

	if privBits != pubBits {
		log.Error(privBits, " != ", pubBits)
		ec.Check(errors.New("BitLength of keys does not match"))
	}
	bits := int(privBits)
	log.Debug("bits: ", bits)

	config := packet.Config{
		DefaultHash:            crypto.SHA256,
		DefaultCipher:          packet.CipherAES256,
		DefaultCompressionAlgo: packet.CompressionZLIB,
		CompressionConfig: &packet.CompressionConfig{
			Level: 9,
		},
		RSABits: bits,
	}
	currentTime := config.Now()
	uid := packet.NewUserId("", "", "")

	e := openpgp.Entity{
		PrimaryKey: pubKey,
		PrivateKey: privKey,
		Identities: make(map[string]*openpgp.Identity),
	}
	isPrimaryId := false

	e.Identities[uid.Id] = &openpgp.Identity{
		Name:   uid.Name,
		UserId: uid,
		SelfSignature: &packet.Signature{
			CreationTime: currentTime,
			SigType:      packet.SigTypePositiveCert,
			PubKeyAlgo:   packet.PubKeyAlgoRSA,
			Hash:         config.Hash(),
			IsPrimaryId:  &isPrimaryId,
			FlagsValid:   true,
			FlagSign:     true,
			FlagCertify:  true,
			IssuerKeyId:  &e.PrimaryKey.KeyId,
		},
	}

	keyLifetimeSecs := uint32(86400 * 365)

	e.Subkeys = make([]openpgp.Subkey, 1)
	e.Subkeys[0] = openpgp.Subkey{
		PublicKey:  pubKey,
		PrivateKey: privKey,
		Sig: &packet.Signature{
			CreationTime:              currentTime,
			SigType:                   packet.SigTypeSubkeyBinding,
			PubKeyAlgo:                packet.PubKeyAlgoRSA,
			Hash:                      config.Hash(),
			PreferredHash:             []uint8{8}, // SHA-256
			FlagsValid:                true,
			FlagEncryptStorage:        true,
			FlagEncryptCommunications: true,
			IssuerKeyId:               &e.PrimaryKey.KeyId,
			KeyLifetimeSecs:           &keyLifetimeSecs,
		},
	}
	return &e
}
