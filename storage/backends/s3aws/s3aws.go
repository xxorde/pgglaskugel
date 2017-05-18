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

package s3aws

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	minio "github.com/minio/minio-go"
	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/backup"
	"github.com/xxorde/pgglaskugel/util"
)

var (
	//timeout  = time.Hour * 48
	timeout               = time.Duration(0)
	extractTimeFromBackup = regexp.MustCompile(`.*@`) // Regexp to remove the name from a backup
)

// Uploads a file to S3 given a bucket and object key. Also takes a duration
// value to terminate the update if it doesn't complete within that time.
//
// The AWS Region needs to be provided in the AWS shared config or on the
// environment variable as `AWS_REGION`. Credentials also must be provided
// Will default to shared config file, but can load from environment if provided.
//
// Usage:
//   # Upload myfile.txt to myBucket/myKey. Must complete within 10 minutes or will fail
//   go run withContext.go -b mybucket -k myKey -d 10m < myfile.txt
type reader struct {
	r io.Reader
}

func (r *reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

// S3backend returns a struct to use the S3-Methods
type S3backend struct {
}

// GetWals returns WAL-Files from S3
func (b S3backend) GetWals(viper *viper.Viper) (a backup.Archive, err error) {
	log.Debug("Get backups from S3")
	// Initialize minio client object.
	a.MinioClient = b.getS3Connection(viper)
	a.Bucket = viper.GetString("s3_bucket_wal")
	bn := viper.GetString("backup_to")
	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	isRecursive := true
	objectCh := a.MinioClient.ListObjects(a.Bucket, "", isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return a, err
		}
		log.Debug(object)
		if object.Err != nil {
			return a, err
		}

		err = a.Add(object.Key, bn, object.Size)
		if err != nil {
			return a, err
		}

	}
	return a, nil
}

// GetBackups returns Backups
func (b S3backend) GetBackups(viper *viper.Viper, subDirWal string) (backups backup.Backups) {
	log.Debug("Get backups from S3")
	// Initialize minio client object.
	minioClient := b.getS3Connection(viper)
	backups.WalPath = viper.GetString("s3_bucket_wal")
	bucket := viper.GetString("s3_bucket_backup")
	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	isRecursive := true
	objectCh := minioClient.ListObjects(bucket, "", isRecursive, doneCh)
	for object := range objectCh {
		var newBackup backup.Backup
		var err error
		if object.Err != nil {
			log.Error(object.Err)
		}
		log.Debug(object)

		newBackup.Path = bucket
		newBackup.Extension = filepath.Ext(object.Key)

		// Get Name without suffix
		newBackup.Name = strings.TrimSuffix(object.Key, newBackup.Extension)
		newBackup.Size = object.Size

		// Get the time from backup name
		backupTimeRaw := extractTimeFromBackup.ReplaceAllString(newBackup.Name, "${1}")
		newBackup.Created, err = time.Parse(backup.BackupTimeFormat, backupTimeRaw)
		if err != nil {
			log.Error(err)
		}
		// Add back reference to the list of backups
		newBackup.Backups = &backups
		backups.Backup = append(backups.Backup, newBackup)
	}
	// Sort backups
	backups.Sort()
	return backups

}

// GetConnection returns an S3-Connection Handler
func (b S3backend) getS3Connection(viper *viper.Viper) (minioClient minio.Client) {
	endpoint := viper.GetString("s3_endpoint")
	accessKeyID := viper.GetString("s3_access_key")
	secretAccessKey := viper.GetString("s3_secret_key")
	ssl := viper.GetBool("s3_ssl")
	version := viper.GetInt("s3_protocol_version")

	var client *minio.Client
	var err error

	// Initialize minio client object.
	switch version {
	case 4:
		log.Debug("Using S3 version 4")
		client, err = minio.NewV4(endpoint, accessKeyID, secretAccessKey, ssl)
	case 2:
		log.Debug("Using S3 version 2")
		client, err = minio.NewV2(endpoint, accessKeyID, secretAccessKey, ssl)
	default:
		log.Debug("Autodetecting S3 version")
		client, err = minio.New(endpoint, accessKeyID, secretAccessKey, ssl)
	}
	if err != nil {
		log.Fatal(err)
	}

	client.SetAppInfo(viper.GetString("myname"), viper.GetString("version"))
	log.Debug("minioClient: ", minioClient)

	return *client
}

// WriteStream handles a stream and writes it to S3 storage
func (b S3backend) WriteStream(viper *viper.Viper, input *io.Reader, name string, backuptype string) {
	var bucket string
	if backuptype == "basebackup" {
		bucket = viper.GetString("s3_bucket_backup")
	} else if backuptype == "archive" {
		bucket = viper.GetString("s3_bucket_wal")
	} else {
		log.Fatalf(" unknown stream-type: %s\n", backuptype)
	}
	endpoint := viper.GetString("s3_endpoint")
	location := viper.GetString("s3_location")
	disableSsl := viper.GetBool("s3_ssl")
	S3ForcePathStyle := viper.GetBool("s3_force_path_style")
	partSize := int64(1024 * 1024 * viper.GetInt("s3_part_size_mb"))
	contentType := "zstd"

	// Set contentType for encryption
	if viper.GetBool("encrypt") {
		contentType = "pgp"
	}

	// Initialize minio client object.
	minioClient := b.getS3Connection(viper)

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		log.Debugf("Bucket already exists, we are using it: %s", bucket)
	} else {
		// Try to create bucket
		err = minioClient.MakeBucket(bucket, location)
		if err != nil {
			log.Debug("minioClient.MakeBucket(bucket, location) failed")
			log.Fatal(err)
		}
		log.Infof("Bucket %s created.", bucket)
	}

	// Prepare connection with aws-sdk
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           &location,
		Endpoint:         &endpoint,
		DisableSSL:       &disableSsl,
		S3ForcePathStyle: &S3ForcePathStyle,
		Credentials:      credentials.NewStaticCredentials(viper.GetString("s3_access_key"), viper.GetString("s3_secret_key"), "123"),
	}))

	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = partSize
	})

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	cancelFn := func() {
	}
	if timeout > 0 {
		log.Debug("Timeout will be used: ", timeout)
		ctx, cancelFn = context.WithTimeout(ctx, timeout*time.Second)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	defer cancelFn()

	log.Debug("Put stream into bucket: ", bucket)
	_, err = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:      &bucket,
		Key:         &name,
		Body:        &reader{*input},
		ContentType: &contentType,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			log.Fatalf("upload canceled due to timeout, %v\n", err)
		} else {
			log.Fatalf("failed to upload object, %v\n", err)
		}
	}
}

// Fetch recover from a S3 compatible object store
func (b S3backend) Fetch(viper *viper.Viper) (err error) {
	bucket := viper.GetString("s3_bucket_wal")
	walTarget := viper.GetString("waltarget")
	walName := viper.GetString("walname")
	walSource := walName + ".zst"
	encrypt := viper.GetBool("encrypt")
	// Initialize minio client object.
	minioClient := b.getS3Connection(viper)
	cmdZstd := viper.GetString("path_to_zstd")
	cmdGpg := viper.GetString("path_to_gpg")

	log.Debug("fetchFromS3 walTarget: ", walTarget, " walName: ", walName)
	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Error("Can not test for S3 bucket")
		log.Fatal(err)
	}
	if !exists {
		log.Fatal("Bucket to fetch from does not exists")
	}

	walObject, err := minioClient.GetObject(bucket, walSource)
	if err != nil {
		log.Error("Can not get WAL file from S3")
		log.Fatal(err)
	}
	defer walObject.Close()

	// Test if the object is accessible
	stat, err := walObject.Stat()
	if err != nil {
		log.Error("Can not get stats for WAL file from S3, does WAL file exists?")
		log.Fatal(err)
	}
	if stat.Size <= 0 {
		log.Fatal("WAL object has size <= 0")
	}

	var gpgStout io.ReadCloser
	if encrypt || stat.ContentType == "pgp" {
		log.Debug("content type: ", stat.ContentType)
		// We need to decrypt the wal file

		// Decrypt the compressed data
		gpgCmd := exec.Command(cmdGpg, "--decrypt", "-o", "-")
		// Set the decryption output as input for inflation
		gpgStout, err = gpgCmd.StdoutPipe()
		if err != nil {
			log.Fatal("Can not attach pipe to gpg process, ", err)
		}
		// Attach output of WAL to stdin
		gpgCmd.Stdin = walObject
		// Watch output on stderror
		gpgStderror, err := gpgCmd.StderrPipe()
		util.Check(err)
		go util.WatchOutput(gpgStderror, log.Info, nil)

		// Start decryption
		if err := gpgCmd.Start(); err != nil {
			log.Fatal("gpg failed on startup, ", err)
		}
		log.Debug("gpg started")
	}

	// command to inflate the data stream
	inflateCmd := exec.Command(cmdZstd, "-d", "-o", walTarget)

	// Watch output on stderror
	inflateDone := make(chan struct{}) // Channel to wait for WatchOutput
	inflateStderror, err := inflateCmd.StderrPipe()
	util.Check(err)
	go util.WatchOutput(inflateStderror, log.Info, inflateDone)

	// Assign inflationInput as Stdin for the inflate command
	if gpgStout != nil {
		// If gpgStout is defined use it as input
		inflateCmd.Stdin = gpgStout
	} else {
		// If gpgStout is not defined use raw WAL
		inflateCmd.Stdin = walObject
	}

	// Start WAL inflation
	if err := inflateCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Debug("Inflation started")

	// Wait for watch goroutine before Cmd.Wait(), race condition!
	<-inflateDone

	// If there is still data in the output pipe it can be lost!
	err = inflateCmd.Wait()
	util.CheckCustom(err, "Inflation failed after startup, ")
	return err
}

// GetBasebackup gets things from S3
func (b S3backend) GetBasebackup(viper *viper.Viper, backup *backup.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	log.Debug("getFromS3")
	bucket := viper.GetString("s3_bucket_backup")

	// Initialize minio client object.
	minioClient := b.getS3Connection(viper)

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

// DeleteAll deletes all backups in the struct
func (b S3backend) DeleteAll(backups *backup.Backups) (count int, err error) {
	// Sort backups
	backups.SortDesc()
	// We delete all backups, but start with the oldest just in case
	for i := len(backups.Backup) - 1; i >= 0; i-- {
		backup := backups.Backup[i]
		err = backups.MinioClient.RemoveObject(backup.Path, backup.Name+backup.Extension)
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
func (b S3backend) GetStartWalLocation(bp *backup.Backup) (startWalLocation string, err error) {
	// Escape the name so we can use it in a regular expression
	searchName := regexp.QuoteMeta(bp.Name)
	// Regex to identify the right file
	regLabel := regexp.MustCompile(`.*LABEL: ` + searchName)
	log.Debug("regLabel: ", regLabel)

	log.Debug("Looking for the backup label that contains: ", searchName)

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	isRecursive := true
	objectCh := bp.Backups.MinioClient.ListObjects(bp.Backups.WalPath, "", isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Error(object.Err)
		}

		// log.Debug("Looking at potential backup label: ", object.Key)

		if object.Size > backup.MaxBackupLabelSize {
			// size is to big for backup label
			// log.Debug("Object is to big to be a backup label, size: ", object.Size)
			continue
		}

		if backup.RegBackupLabelFile.MatchString(object.Key) {
			log.Debug(object.Key, " => seems to be a backup Label, by size and name")

			backupLabelFile, err := bp.Backups.MinioClient.GetObject(bp.Backups.WalPath, object.Key)
			if err != nil {
				log.Warn("Can not get backupLabel, ", err)
				continue
			}

			bufCompressed := make([]byte, backup.MaxBackupLabelSize)
			readCount, err := backupLabelFile.Read(bufCompressed)
			if err != nil && err != io.EOF {
				log.Warn("Can not read backupLabel, ", err)
				continue
			}
			log.Debug("Read ", readCount, " from backupLabel")

			// Command to decompress the backuplabel
			catCmd := exec.Command("zstd", "-d", "--stdout")
			catCmdStdout, err := catCmd.StdoutPipe()
			if err != nil {
				// if we can not open the file we continue with next
				log.Warn("catCmd.StdoutPipe(), ", err)
				continue
			}

			// Use backupLabel as input for catCmd
			catDone := make(chan struct{}) // Channel to wait for WatchOutput
			catCmd.Stdin = bytes.NewReader(bufCompressed)
			catCmdStderror, err := catCmd.StderrPipe()
			go util.WatchOutput(catCmdStderror, log.Debug, catDone)

			err = catCmd.Start()
			if err != nil {
				log.Warn("catCmd.Start(), ", err)
				continue
			}

			bufPlain, err := ioutil.ReadAll(catCmdStdout)
			if err != nil {
				log.Warn("Reading from command: ", err)
				continue
			}

			// Wait for output watchers to finish
			// If the Cmd.Wait() is called while another process is reading
			// from Stdout / Stderr this is a race condition.
			// So we are waiting for the watchers first
			<-catDone

			// Wait for the command to finish
			err = catCmd.Wait()
			if err != nil {
				// We ignore errors here, zstd returns 1 even if everything is fine here
				log.Debug("catCmd.Wait(), ", err)
			}
			log.Debug("Backuplabel:\n", string(bufPlain))

			if len(regLabel.Find(bufPlain)) > 1 {
				log.Debug("Found matching backup label")
				bp, err = backup.ParseBackupLabel(bp, bufPlain)
				if err != nil {
					log.Error(err)
				}
				bp.LabelFile = object.Key
				return bp.StartWalLocation, err
			}
		}
	}
	return "", errors.New("START WAL LOCATION not found")
}

// DeleteWal deletes the given WAL-file
func (b S3backend) DeleteWal(viper *viper.Viper, w *backup.Wal) (err error) {
	minioClient := b.getS3Connection(viper)
	err = minioClient.RemoveObject(w.Archive.Bucket, w.Name+w.Extension)
	if err != nil {
		log.Warn(err)
	}
	return err
}
