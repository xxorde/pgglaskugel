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
	"io"
	"io/ioutil"
	"os/exec"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"os"

	log "github.com/Sirupsen/logrus"
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	util "github.com/xxorde/pgglaskugel/util"
)

// restoreCmd represents the restore command
var (
	restoreCmd = &cobra.Command{
		Use:   "restore [BACKUPNAME] [DESTINATION]",
		Short: "Restore an existing backup to a given location",
		Long:  `Restore an existing backup to a given location.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.Debug("restore called")
			backupName := viper.GetString("backup")
			backupDestination := viper.GetString("restore-to")
			writeRecoveryConf := viper.GetBool("write-recovery-conf")
			force := viper.GetBool("force-restore")

			// TODO This is not very robust, maybe we find a better way here
			// Set backupName if given directly
			if len(args) >= 1 {
				backupName = args[0]
			}

			// Set backupDestination if given directly
			if len(args) >= 2 {
				backupDestination = args[1]
			}

			// Too many arguments
			if len(args) > 2 {
				log.Fatal("Too many arguments: ", args)
			}

			if backupName == "" {
				log.Fatal("Backupname not set")
			}

			// If target directory does not exists ...
			if exists, err := util.Exists(backupDestination); !exists || err != nil {
				log.Info(backupDestination, " does not exists, create it")
				err := os.MkdirAll(backupDestination, 0700)
				if err != nil {
					log.Fatal(err)
				}
			}

			// If backup folder is not empty, ask what to do (and force is not set)
			if empty, err := util.IsEmpty(backupDestination); (!empty || err != nil) && force != true {
				force, err := util.AnswerConfirmation("Destination directory is not empty, continue anyway?")
				if err != nil {
					log.Error(err)
				}
				if force != true {
					log.Fatal(backupDestination + " is not an empty directory, you need to use force")
				}
			}

			log.Info("Going to restore backup '", backupName, "' to: ", backupDestination)
			err := restoreBasebackup(backupDestination, backupName)
			if err != nil {
				log.Fatal(err)
			}

			if writeRecoveryConf {
				// When no restore_command command set, set it
				if viper.GetString("restore_command") == "" {
					// Include config file in potential restore_command command
					configOption := ""
					if viper.ConfigFileUsed() != "" {
						configOption = " --config " + viper.ConfigFileUsed()
					}

					// Preset restore_command
					viper.Set("restore_command", myExecutable+configOption+" fetch %f %p")
				}
				restoreCommand := viper.GetString("restore_command")
				recoveryConf := "# Created by " + myExecutable + "\nrestore_command = '" + restoreCommand + "'"

				log.Info("Going to write recovery.conf to: ", backupDestination)
				err = ioutil.WriteFile(backupDestination+"/recovery.conf", []byte(recoveryConf), 0600)
				if err != nil {
					panic(err)
				}
			}

			printDone()
		},
	}
)

func restoreBasebackup(backupDestination string, backupName string) (err error) {
	backups := getMyBackups()
	backup, err := backups.Find(backupName)
	if err != nil {
		log.Fatal(err)
	}
	encrypt := viper.GetBool("encrypt")
	var dataStream *io.Reader // Stream for the raw backup data

	// Command to inflate the data stream
	// Read from stdin and write do stdout
	inflateCmd := exec.Command(cmdZstd, "-d", "--stdout", "-")

	// Command to untar the uncompressed data stream
	untarCmd := exec.Command("tar", "--extract", "--directory", backupDestination)

	// Attach pipe to the inflation command
	inflateStdout, err := inflateCmd.StdoutPipe()
	if err != nil {
		log.Fatal("Can not attach pipe to backup process, ", err)
	}

	// Watch stderror of inflation
	inflateStderror, err := inflateCmd.StderrPipe()
	ec.Check(err)
	go util.WatchOutput(inflateStderror, log.Info)

	// Watch stderror of untar
	untarStderror, err := untarCmd.StderrPipe()
	ec.Check(err)
	go util.WatchOutput(untarStderror, log.Info)

	// Pipe the the inflated backup in untar
	untarCmd.Stdin = inflateStdout

	// If encryption is used, pipe data through decryption before inflation
	// Command to decrypt the compressed data
	gpgCmd := exec.Command(cmdGpg, "--decrypt", "-o", "-")
	// TODO: Test if backup is encrypted
	if encrypt {
		log.Debug("Backup will be decrypted")

		// Set the decryption output as input for inflation
		inflateCmd.Stdin, err = gpgCmd.StdoutPipe()
		if err != nil {
			log.Fatal("Can not attach pipe to gpg process, ", err)
		}
		// Attach output of backup to stdin
		dataStream = &gpgCmd.Stdin
		// Watch output on stderror
		gpgStderror, err := gpgCmd.StderrPipe()
		ec.Check(err)
		go util.WatchOutput(gpgStderror, log.Info)
	} else {
		log.Debug("Backup will not be encrypted")
		// Encryption is not used, inflate data stream
		dataStream = &inflateCmd.Stdin
	}

	// WaitGroups for workers
	var wgRestoreStart sync.WaitGroup // Waiting group to wait for start
	var wgRestoreDone sync.WaitGroup  // Waiting group to wait for end

	// Add one to the waiting counter
	wgRestoreStart.Add(1)

	// Start getBasebackup in new go-routine
	go getBasebackup(backup, dataStream, &wgRestoreStart, &wgRestoreDone)

	// Wait for getBasebackup to start
	wgRestoreStart.Wait()

	// Start untar
	if err := untarCmd.Start(); err != nil {
		log.Fatal("untarCmd failed on startup, ", err)
	}
	log.Info("Untar started")

	// Start WAL inflation
	if err := inflateCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Debug("Inflation started")

	if encrypt {
		log.Debug("Start decryption go-routine")
		if err := gpgCmd.Start(); err != nil {
			log.Fatal("gpg failed on startup, ", err)
		}
		log.Debug("gpg started")
	}

	// Wait for compression to finish
	// Using new error variables here due to previous race conditions
	errWait := inflateCmd.Wait()
	if errWait != nil {
		log.Fatal("inflateCmd failed after startup, ", err)
	}

	// WAIT! If there is still data in the output pipe it can be lost!
	// Wait for backup to finish
	errWait = untarCmd.Wait()
	if errWait != nil {
		log.Fatal("untarCmd failed after startup, ", err)
	}
	log.Debug("untarCmd done")

	// Tell the backup source that the chain is finished
	wgRestoreDone.Done()
	return err
}

func getBasebackup(backup *util.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	// Add one worker to wait for finish
	wgDone.Add(1)

	storageType := backup.StorageType()
	switch storageType {
	case "file":
		getFromFile(backup, backupStream, wgStart, wgDone)
	case "s3":
		getFromS3(backup, backupStream, wgStart, wgDone)
	default:
		log.Fatal(storageType, " no valid value for backup_to")
	}
}

func getFromFile(backup *util.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
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

func getFromS3(backup *util.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	log.Debug("getFromS3")
	bucket := viper.GetString("s3_bucket_backup")

	// Initialize minio client object.
	minioClient := getS3Connection()

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Fatal(err)
	}
	if !exists {
		log.Fatal("Bucket to restore from does not exists")
	}

	backupSource := backup.Name + backup.Extension
	backupObject, err := minioClient.GetObject(bucket, backupSource)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer backupObject.Close()

	// Test if the object is accessible
	stat, err := backupObject.Stat()
	if err != nil {
		log.Fatal(err)
	}
	if stat.Size <= 0 {
		log.Fatal("Backup object has size <= 0")
	}

	// Assign backupObject as input to the restore stack
	*backupStream = backupObject

	// Tell the chain that backupStream can access data now
	wgStart.Done()

	// Wait for the rest of the chain to finish
	log.Debug("getFromS3 waits for the rest of the chain to finish")
	wgDone.Wait()
	log.Debug("getFromS3 done")
}

func init() {
	RootCmd.AddCommand(restoreCmd)
	restoreCmd.PersistentFlags().StringP("backup", "B", "", "The backup to restore")
	restoreCmd.PersistentFlags().String("restore-to", "/var/lib/postgresql/pgGlaskugel-restore", "The destination to restore to")
	restoreCmd.PersistentFlags().Bool("write-recovery-conf", true, "Automatic create a recovery.conf to replay WAL from archive")
	restoreCmd.PersistentFlags().Bool("force-restore", false, "Force the deletion of existing data (danger zone)!")

	// Bind flags to viper
	viper.BindPFlag("backup", restoreCmd.PersistentFlags().Lookup("backup"))
	viper.BindPFlag("restore-to", restoreCmd.PersistentFlags().Lookup("restore-to"))
	viper.BindPFlag("write-recovery-conf", restoreCmd.PersistentFlags().Lookup("write-recovery-conf"))
	viper.BindPFlag("force-restore", restoreCmd.PersistentFlags().Lookup("force-restore"))
}
