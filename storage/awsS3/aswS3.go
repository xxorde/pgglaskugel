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

package awsS3

import (
	"context"
	"io"
	"os/exec"
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
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	"github.com/xxorde/pgglaskugel/util"
	"github.com/xxorde/pgglaskugel/wal"
)

var (
	//timeout  = time.Hour * 48
	timeout = time.Duration(0)
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
func (b S3backend) GetWals(viper func() map[string]interface{}) (archive wal.Archive) {
	log.Debug("Get backups from S3")
	// Initialize minio client object.
	archive.MinioClient = b.getS3Connection(viper)
	archive.Bucket = viper()["s3_bucket_wal"].(string)
	archive.GetWals()
	return archive
}

// GetBackups returns Backups
func (b S3backend) GetBackups(viper func() map[string]interface{}, subDirWal string) (backups util.Backups) {
	log.Debug("Get backups from S3")
	// Initialize minio client object.
	backups.MinioClient = b.getS3Connection(viper)
	backups.GetBackupsInBucket(viper()["s3_bucket_backup"].(string))
	backups.WalBucket = viper()["s3_bucket_wal"].(string)
	return backups

}

// GetConnection returns an S3-Connection Handler
func (b S3backend) getS3Connection(viper func() map[string]interface{}) (minioClient minio.Client) {
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

// WriteStream handles a stream and writes it to S3 storage
func (b S3backend) WriteStream(viper func() map[string]interface{}, input *io.Reader, name string, backuptype string) {
	var bucket string
	if backuptype == "basebackup" {
		bucket = viper()["s3_bucket_backup"].(string)
	} else if backuptype == "archive" {
		bucket = viper()["s3_bucket_wal"].(string)
	} else {
		log.Fatalf(" unknown stream-type: %s\n", backuptype)
	}
	endpoint := viper()["s3_endpoint"].(string)
	location := viper()["s3_location"].(string)
	disableSsl := !viper()["s3_ssl"].(bool)
	encrypt := viper()["encrypt"].(bool)
	S3ForcePathStyle := false
	partSize := int64(1024 * 1024 * viper()["s3_part_size_mb"].(int))
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

	// Prepare connection with aws-sdk
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           &location,
		Endpoint:         &endpoint,
		DisableSSL:       &disableSsl,
		S3ForcePathStyle: &S3ForcePathStyle,
		Credentials:      credentials.NewStaticCredentials(viper()["s3_access_key"].(string), viper()["s3_secret_key"].(string), "123"),
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
func (b S3backend) Fetch(viper func() map[string]interface{}) (err error) {
	bucket := viper()["s3_bucket_wal"].(string)
	walTarget := viper()["waltarget"].(string)
	walName := viper()["walname"].(string)
	walSource := walName + ".zst"
	encrypt := viper()["encrypt"].(bool)
	// Initialize minio client object.
	minioClient := b.getS3Connection(viper)
	cmdZstd := viper()["path_to_zstd"].(string)
	cmdGpg := viper()["path_to_gpg"].(string)

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

// GetBasebackup gets things from S3
func (b S3backend) GetBasebackup(viper func() map[string]interface{}, backup *util.Backup, backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
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
