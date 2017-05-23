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

package file

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/backup"
	"github.com/xxorde/pgglaskugel/util"
)

// Localbackend defines a struct to use the file-methods
type Localbackend struct {
}

// GetBackups returns backups
func (b Localbackend) GetBackups(viper *viper.Viper, subDirWal string) (bp backup.Backups) {
	log.Debug("Get backups from folder: ", viper.GetString("backupdir"))
	backupDir := viper.GetString("backupdir")
	files, _ := ioutil.ReadDir(backupDir)
	for _, f := range files {
		var newBackup backup.Backup
		var err error
		path := filepath.Join(backupDir, f.Name())
		newBackup.Path, err = filepath.Abs(path)
		if err != nil {
			log.Warn(err)
		}
		newBackup.Extension = filepath.Ext(path)
		// Get the name of the backup file without the extension
		newBackup.Name = strings.TrimSuffix(filepath.Base(path), newBackup.Extension)

		// Get size of backup
		file, err := os.Open(path)
		if err != nil {
			log.Warn(err)
		}
		defer file.Close()

		fi, err := file.Stat()
		if err != nil {
			log.Warn(err)
		}
		newBackup.Size = fi.Size()

		// Remove anything before the '@'
		reg := regexp.MustCompile(`.*@`)
		backupTimeRaw := reg.ReplaceAllString(newBackup.Name, "${1}")
		newBackup.Created, err = time.Parse(backup.BackupTimeFormat, backupTimeRaw)
		if err != nil {
			log.Warn(err)
		}
		// Add back reference to the list of backups
		newBackup.Backups = &bp
		bp.Backup = append(bp.Backup, newBackup)
	}
	// Sort backups
	bp.Sort()
	return bp
}

//GetWals returns Wals
func (b Localbackend) GetWals(viper *viper.Viper) (a backup.Archive, err error) {
	// Get WAL files from filesystem
	log.Debug("Get WAL from folder: ", viper.GetString("waldir"))
	a.Path = viper.GetString("waldir")
	bn := viper.GetString("backup_to")
	// WAL files are load sequential from file system.
	files, err := ioutil.ReadDir(a.Path)
	if err != nil {
		return a, err
	}
	for _, f := range files {
		size := f.Size()
		err = a.Add(f.Name(), bn, size)
		if err != nil {
			return a, err

		}
	}
	return a, nil
}

// WriteStream handles a stream and writes it to a local file
func (b Localbackend) WriteStream(viper *viper.Viper, input *io.Reader, name string, backuptype string) {
	var backuppath string
	if backuptype == "basebackup" {
		backuppath = filepath.Join(viper.GetString("backupdir"), name)
	} else if backuptype == "archive" {
		backuppath = filepath.Join(viper.GetString("waldir"), name)
	} else {
		log.Fatalf(" unknown stream-type: %s\n", backuptype)
	}
	file, err := os.OpenFile(backuppath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		log.Fatal("Can not create output file, ", err)
	}
	defer file.Close()

	log.Debug("Start writing to file")
	written, err := io.Copy(file, *input)
	if err != nil {
		log.Fatalf("writeStreamToFile: Error while writing to %s, written %d, error: %v", backuppath, written, err)
	}

	log.Infof("%d bytes were written, waiting for file.Sync()", written)
	log.Debug("Wait for file.Sync()", backuppath)
	file.Sync()
	log.Debug("Done waiting for file.Sync()", backuppath)
}

// Fetch uses the shell command zstd to recover WAL files
func (b Localbackend) Fetch(viper *viper.Viper) (err error) {
	walTarget := viper.GetString("waltarget")
	walName := viper.GetString("walname")
	walSource := filepath.Join(viper.GetString("archivedir"), "/wal/", walName+".zst")
	encrypt := viper.GetBool("encrypt")
	cmdZstd := viper.GetString("path_to_zstd")
	cmdGpg := viper.GetString("path_to_gpg")

	log.Debug("fetchFromFile, walTarget: ", walTarget, ", walName: ", walName, ", walSource: ", walSource)

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
	util.Check(err)
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
	util.Check(err)
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
	util.CheckCustom(err, "Inflation failed after startup")

	return err
}

//GetBasebackup Gets backups from file
func (b Localbackend) GetBasebackup(viper *viper.Viper, backup *backup.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	log.Debug("getFromFile")
	file, err := os.Open(backup.Path)
	util.Check(err)
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

// DeleteAll deletes all backups in the struct
func (b Localbackend) DeleteAll(viper *viper.Viper, backups *backup.Backups) (count int, err error) {
	// Sort backups
	backups.SortDesc()
	// We delete all backups, but start with the oldest just in case
	for i := len(backups.Backup) - 1; i >= 0; i-- {
		backup := backups.Backup[i]
		err = os.Remove(backup.Path)
		if err != nil {
			log.Warn(err)
		} else {
			count++
		}

	}
	return count, err
}

// GetStartWalLocation returns the oldest needed WAL file
// Every older WAL file is not required to use this backup
func (b Localbackend) GetStartWalLocation(viper *viper.Viper, bp *backup.Backup) (startWalLocation string, err error) {
	// Escape the name so we can use it in a regular expression
	searchName := regexp.QuoteMeta(bp.Name)
	// Regex to identify the right file
	regLabel := regexp.MustCompile(`.*LABEL: ` + searchName)
	log.Debug("regLabel: ", regLabel)
	encrypt := viper.GetBool("encrypt")
	cmdZstdcat := viper.GetString("path_to_zstdcat")
	cmdZstd := viper.GetString("path_to_zstd")
	cmdGpg := viper.GetString("path_to_gpg")

	bp.Backups.WalPath = viper.GetString("waldir")

	files, _ := ioutil.ReadDir(bp.Backups.WalPath)
	// find all backup labels
	for _, f := range files {
		if f.Size() > backup.MaxBackupLabelSize {
			// size is to big for backup label
			continue
		}
		if backup.RegBackupLabelFile.MatchString(f.Name()) {
			log.Debug(f.Name(), " => seems to be a backup Label, by size and name")
			labelFile := filepath.Join(bp.Backups.WalPath, f.Name())
			var backupLabel []byte

			// If encryption is not used the restore is easy
			if encrypt == false {
				catCmd := exec.Command(cmdZstdcat, labelFile)

				catCmdStdout, err := catCmd.StdoutPipe()
				if err != nil {
					// if we can not open the file we continue with next
					log.Warn("catCmd.StdoutPipe(), ", err)
					continue
				}

				err = catCmd.Start()
				if err != nil {
					log.Warn("catCmd.Start(), ", err)
					continue
				}

				backupLabel, err = ioutil.ReadAll(catCmdStdout)
				if err != nil {
					log.Warn("Reading from command: ", err)
					continue
				}

				err = catCmd.Wait()
				if err != nil {
					log.Warn("catCmd.Wait(), ", err)
					continue
				}
			} else {
				// Encryption is used so we have to decrypt
				log.Debug("Label file will be decrypted")

				// Read and decrypt the compressed data
				gpgCmd := exec.Command(cmdGpg, "--decrypt", "-o", "-", labelFile)
				// Set the decryption output as input for inflation
				var gpgStout io.ReadCloser
				gpgStout, err = gpgCmd.StdoutPipe()
				if err != nil {
					log.Fatal("Can not attach pipe to gpg process, ", err)
				}

				// Watch output on stderror
				gpgStderror, err := gpgCmd.StderrPipe()
				util.Check(err)
				go util.WatchOutput(gpgStderror, log.Info, nil)

				// Start decryption
				if err := gpgCmd.Start(); err != nil {
					log.Fatal("gpg failed on startup, ", err)
				}
				log.Debug("gpg started")

				// command to inflate the data stream
				inflateCmd := exec.Command(cmdZstd, "-d", "--stdout")

				// Watch output on stderror
				inflateDone := make(chan struct{}) // Channel to wait for WatchOutput

				inflateStderror, err := inflateCmd.StderrPipe()
				util.Check(err)
				go util.WatchOutput(inflateStderror, log.Info, inflateDone)

				// Assign inflationInput as Stdin for the inflate command
				inflateCmd.Stdin = gpgStout

				// Expose inflateCmd output as output
				inflateStout, err := inflateCmd.StdoutPipe()
				util.Check(err)

				// Start WAL inflation
				log.Debug("Going to start inflation...")
				if err := inflateCmd.Start(); err != nil {
					log.Fatal("zstd failed on startup, ", err)
				}
				log.Debug("Inflation started")

				// Write data to variable backupLabel
				backupLabel = util.StreamToByte(inflateStout)

				// Wait for watch goroutine before Cmd.Wait(), race condition!
				<-inflateDone

				// If there is still data in the output pipe it can be lost!
				err = inflateCmd.Wait()
				util.CheckCustom(err, "Inflation failed after startup")
			}
			log.Debugf("backupLabel: %s,\nsize: %d", backupLabel, len(backupLabel))

			if len(regLabel.Find(backupLabel)) > 1 {
				log.Debug("Found matching backup label file: ", f.Name())
				err = bp.ParseBackupLabel(backupLabel)
				if err != nil {
					log.Error(backupLabel)
				}
				return bp.StartWalLocation, err
			}
		}
	}
	return "", errors.New("START WAL LOCATION not found")
}

// DeleteWal deletes the given WAL-file
func (b Localbackend) DeleteWal(viper *viper.Viper, w *backup.Wal) (err error) {
	err = os.Remove(filepath.Join(w.Archive.Path, w.Name+w.Extension))
	if err != nil {
		log.Warn(err)
	}
	return err
}
