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

package local

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	log "github.com/Sirupsen/logrus"
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	"github.com/xxorde/pgglaskugel/util"
	"github.com/xxorde/pgglaskugel/wal"
)

type localbackend struct {
}

func New() localbackend {
	var backend localbackend
	return backend
}

// GetFileBackups returns backups
func (b localbackend) GetBackups(viper func() map[string]interface{}, subDirWal string) (backups util.Backups) {
	log.Debug("Get backups from folder: ", viper()["backupdir"])
	backups.GetBackupsInDir(viper()["backupdir"].(string))
	backups.WalDir = filepath.Join(viper()["archivedir"].(string), subDirWal)
	return backups
}

//GetFileWals returns Wals
func (b localbackend) GetWals(viper func() map[string]interface{}) (archive wal.Archive) {
	// Get WAL files from filesystem
	log.Debug("Get WAL from folder: ", viper()["waldir"].(string))
	archive.Path = viper()["waldir"].(string)
	archive.GetWals()
	return archive
}

// WriteStreamToFile handles a stream and writes it to a local file
func (b localbackend) WriteStream(viper func() map[string]interface{}, input *io.Reader, name string, backuptype string) {
	filepath := name
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		log.Fatal("Can not create output file, ", err)
	}
	defer file.Close()

	log.Debug("Start writing to file")
	written, err := io.Copy(file, *input)
	if err != nil {
		log.Fatalf("writeStreamToFile: Error while writing to %s, written %d, error: %v", filepath, written, err)
	}

	log.Infof("%d bytes were written, waiting for file.Sync()", written)
	log.Debug("Wait for file.Sync()", filepath)
	file.Sync()
	log.Debug("Done waiting for file.Sync()", filepath)
}

// FetchFromFile uses the shell command zstd to recover WAL files
func (b localbackend) Fetch(viper func() map[string]interface{}, walTarget string, walName string) (err error) {
	walSource := viper()["archivedir"].(string) + "/wal/" + walName + ".zst"
	log.Debug("fetchFromFile, walTarget: ", walTarget, ", walName: ", walName, ", walSource: ", walSource)
	encrypt := viper()["encrypt"].(bool)
	cmdZstd := viper()["path_to_zstd"].(string)
	cmdGpg := viper()["path_to_gpg"].(string)

	// If encryption is not used the restore is easy
	if encrypt == false {
		fetchCmd := exec.Command(cmdZstd, "-d", walSource, "-o", walTarget)
		err = fetchCmd.Run()
		return err
	}

	// If we reach this code path encryption is turned on
	// Encryption is used so we have to decrypt
	log.Debug("WAL file will be decrypted")

	// Read and decrypt the compressed data
	gpgCmd := exec.Command(cmdGpg, "--decrypt", "-o", "-", walSource)
	// Set the decryption output as input for inflation
	var gpgStout io.ReadCloser
	gpgStout, err = gpgCmd.StdoutPipe()
	if err != nil {
		log.Fatal("Can not attach pipe to gpg process, ", err)
	}

	// Watch output on stderror
	gpgStderror, err := gpgCmd.StderrPipe()
	ec.Check(err)
	go util.WatchOutput(gpgStderror, log.Info, nil)

	// Start decryption
	if err := gpgCmd.Start(); err != nil {
		log.Fatal("gpg failed on startup, ", err)
	}
	log.Debug("gpg started")

	// command to inflate the data stream
	inflateCmd := exec.Command(cmdZstd, "-d", "-o", walTarget)

	// Watch output on stderror
	inflateDone := make(chan struct{}) // Channel to wait for WatchOutput

	inflateStderror, err := inflateCmd.StderrPipe()
	ec.Check(err)
	go util.WatchOutput(inflateStderror, log.Info, inflateDone)

	// Assign inflationInput as Stdin for the inflate command
	inflateCmd.Stdin = gpgStout

	// Start WAL inflation
	if err := inflateCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Debug("Inflation started")

	// Wait for watch goroutine before Cmd.Wait(), race condition!
	<-inflateDone

	// If there is still data in the output pipe it can be lost!
	err = inflateCmd.Wait()
	ec.CheckCustom(err, "Inflation failed after startup")

	return err
}

//GetFromFile Gets backups from file
func (b localbackend) GetBasebackup(viper func() map[string]interface{}, backup *util.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	log.Debug("getFromFile")
	file, err := os.Open(backup.Path)
	ec.Check(err)
	defer file.Close()

	// Set file as backupStream
	*backupStream = file

	// Tell the chain that backupStream can access data now
	wgStart.Done()

	// Wait for the rest of the chain to finish
	log.Debug("getFromFile waits for the rest of the chain to finish")
	wgDone.Wait()
	log.Debug("getFromFile done")

}
