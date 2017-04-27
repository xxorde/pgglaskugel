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

package s3

import (
	"io"
	"os/exec"
	"sync"

	log "github.com/Sirupsen/logrus"
	minio "github.com/minio/minio-go"
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	"github.com/xxorde/pgglaskugel/util"
	"github.com/xxorde/pgglaskugel/wal"
)

type s3backend struct {
}

func New() s3backend {
	var backend s3backend
	return backend
}

// GetS3Wals returns WAL-Files from S3
func (b s3backend) GetWals(viper func() map[string]interface{}) (archive wal.Archive) {
	log.Debug("Get backups from S3")
	// Initialize minio client object.
	archive.MinioClient = b.getS3Connection(viper)
	archive.Bucket = viper()["s3_bucket_wal"].(string)
	archive.GetWals()
	return archive
}

// GetS3Backups returns Backups
func (b s3backend) GetBackups(viper func() map[string]interface{}, subDirWal string) (backups util.Backups) {
	log.Debug("Get backups from S3")
	// Initialize minio client object.
	backups.MinioClient = b.getS3Connection(viper)
	backups.GetBackupsInBucket(viper()["s3_bucket_backup"].(string))
	backups.WalBucket = viper()["s3_bucket_wal"].(string)
	return backups

}

// GetS3Connection returns an S3-Connection Handler
func (b s3backend) getS3Connection(viper func() map[string]interface{}) (minioClient minio.Client) {
	endpoint := viper()["s3_endpoint"].(string)
	accessKeyID := viper()["s3_access_key"].(string)
	secretAccessKey := viper()["s3_secret_key"].(string)
	ssl := viper()["s3_ssl"].(bool)
	version := viper()["s3_protocol_version"].(int)

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

	client.SetAppInfo(viper()["myname"].(string), viper()["version"].(string))
	log.Debug("minioClient: ", minioClient)

	return *client
}

// WriteStreamToS3 handles a stream and writes it to S3 storage
func (b s3backend) WriteStream(viper func() map[string]interface{}, input *io.Reader, name string, backuptype string) {
	location := viper()["s3_location"].(string)
	encrypt := viper()["encrypt"].(bool)
	bucket := viper()["s3_bucket_wal"].(string)
	contentType := "zstd"

	// Set contentType for encryption
	if encrypt {
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

	log.Debug("Put stream into bucket: ", bucket)
	n, err := minioClient.PutObject(bucket, name, *input, contentType)
	if err != nil {
		log.Debug("minioClient.PutObject(", bucket, ", ", name, ", *input,", contentType, ") failed")
		log.Fatal(err)
		return
	}
	log.Infof("Written %d bytes to %s in bucket %s.", n, name, bucket)
}

// FetchFromS3 recover from a S3 compatible object store
func (b s3backend) Fetch(viper func() map[string]interface{}, walTarget string, walName string) (err error) {
	log.Debug("fetchFromS3 walTarget: ", walTarget, " walName: ", walName)
	bucket := viper()["s3_bucket_wal"].(string)
	walSource := walName + ".zst"
	encrypt := viper()["encrypt"].(bool)
	// Initialize minio client object.
	minioClient := b.getS3Connection(viper)
	cmdZstd := viper()["path_to_zstd"].(string)
	cmdGpg := viper()["path_to_gpg"].(string)

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
		ec.Check(err)
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
	ec.Check(err)
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
	ec.CheckCustom(err, "Inflation failed after startup, ")
	return err
}

// GetFromS3 gets things from S3
func (b s3backend) GetBasebackup(viper func() map[string]interface{}, backup *util.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	log.Debug("getFromS3")
	bucket := viper()["s3_bucket_backup"].(string)

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
