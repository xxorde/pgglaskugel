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

package minioCs

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/xml"
	"hash"
	"io"
	"math"
	"os/exec"
	"sort"
	"sync"

	log "github.com/Sirupsen/logrus"
	minio "github.com/minio/minio-go"
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	"github.com/xxorde/pgglaskugel/util"
	"github.com/xxorde/pgglaskugel/wal"
)

const (
	maxPartsCount             = 10000
	maxMultipartPutObjectSize = 1024 * 1024 * 1024 * 640
)

// optimalPartInfo - calculate the optimal part info for
// a given object size.
//
// NOTE: Assumption here is that for any object to be uploaded to
// any S3 compatible object storage it will have the following
// parameters as constants.
func optimalPartInfo(objectSize, minPartSize int64) (totalPartsCount int, partSize int64, lastPartSize int64, err error) {
	// object size is '-1' set it to 640GiB.
	if objectSize == -1 {
		objectSize = maxMultipartPutObjectSize
	}

	// Use floats for part size for all calculations to avoid
	// overflows during float64 to int64 conversions.
	partSizeFlt := math.Ceil(float64(objectSize / maxPartsCount))
	partSizeFlt = math.Ceil(partSizeFlt/float64(minPartSize)) * float64(minPartSize)

	// Total parts count.
	totalPartsCount = int(math.Ceil(float64(objectSize) / partSizeFlt))

	// Part size.
	partSize = int64(partSizeFlt)

	// Last part size.
	lastPartSize = objectSize - int64(totalPartsCount-1)*partSize
	return totalPartsCount, partSize, lastPartSize, nil
}

// completedParts is a collection of parts sortable by their part numbers.
// used for sorting the uploaded parts before completing the multipart request.
type completedParts []minio.CompletePart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// completeMultipartUpload container for completing multipart upload.
type completeMultipartUpload struct {
	XMLName xml.Name             `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUpload" json:"-"`
	Parts   []minio.CompletePart `xml:"Part"`
}

// hashCopyN - Calculates chosen hashes up to partSize amount of bytes.
func hashCopyN(hashAlgorithms map[string]hash.Hash, hashSums map[string][]byte, writer io.Writer, reader io.Reader, partSize int64) (size int64, err error) {
	hashWriter := writer
	for _, v := range hashAlgorithms {
		hashWriter = io.MultiWriter(hashWriter, v)
	}

	// Copies to input at writer.
	size, err = io.CopyN(hashWriter, reader, partSize)
	if err != nil {
		// If not EOF return error right here.
		if err != io.EOF {
			log.Error("io.EOF failed")
			return 0, err
		}
	}

	for k, v := range hashAlgorithms {
		hashSums[k] = v.Sum(nil)
	}
	return size, err
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
	location := viper()["s3_location"].(string)
	encrypt := viper()["encrypt"].(bool)
	contentType := "zstd"
	minPartSize := int64(1024 * 1024 * viper()["s3_part_size_mb"].(int))

	// Set contentType for encryption
	if encrypt {
		contentType = "pgp"
	}

	log.Warn("not jet used contetnType ", contentType)

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

	// Get connection and set in minio.Core to use low level functions
	var c minio.Core
	client := b.getS3Connection(viper)
	c.Client = &client
	metaData := map[string][]string{}

	// Total data read and written to server. should be equal to 'size' at the end of the call.
	var totalUploadedSize int64

	// Complete multipart upload.
	var complMultipartUpload completeMultipartUpload

	// Get the upload id of a previously partially uploaded object or initiate a new multipart upload
	uploadID, err := c.NewMultipartUpload(bucket, name, metaData)
	if err != nil {
		log.Fatal("NewMultipartUpload failed", err)
	}

	size := int64(-1)

	// Calculate the optimal parts info for a given size.
	totalPartsCount, partSize, _, err := optimalPartInfo(size, minPartSize)
	if err != nil {
		log.Fatal("optimalPartInfo failed", err)
	}

	// Initialize parts uploaded map.
	partsInfo := make(map[int]minio.ObjectPart)

	// Part number always starts with '1'.
	partNumber := 1

	// Initialize a temporary buffer.
	tmpBuffer := new(bytes.Buffer)

	for partNumber <= totalPartsCount {
		// Choose hash algorithms to be calculated by hashCopyN, avoid sha256
		// with non-v4 signature request or HTTPS connection
		hashSums := make(map[string][]byte)
		hashAlgos := make(map[string]hash.Hash)
		hashAlgos["md5"] = md5.New()
		hashAlgos["sha256"] = sha256.New()

		// Calculates hash sums while copying partSize bytes into tmpBuffer.
		prtSize, rErr := hashCopyN(hashAlgos, hashSums, tmpBuffer, *input, partSize)
		if rErr != nil && rErr != io.EOF {
			log.Fatal("io.EOF failed", err)
		}

		// Proceed to upload the part.
		var objPart minio.ObjectPart
		objPart, err = c.PutObjectPart(bucket, name, uploadID, partNumber,
			prtSize, tmpBuffer, hashSums["md5"], hashSums["sha256"])
		if err != nil {
			// Reset the temporary buffer upon any error.
			tmpBuffer.Reset()
			log.Fatal("PutObjectPart failed, written ", totalUploadedSize, err)
		}

		// Save successfully uploaded part metadata.
		partsInfo[partNumber] = objPart

		// Reset the temporary buffer.
		tmpBuffer.Reset()

		// Save successfully uploaded size.
		totalUploadedSize += prtSize

		// Increment part number.
		partNumber++

		// For unknown size, Read EOF we break away.
		// We do not have to upload till totalPartsCount.
		if size < 0 && rErr == io.EOF {
			break
		}
	}

	// Verify if we uploaded all the data.
	if size > 0 {
		if totalUploadedSize != size {
			log.Fatal("totalUploadedSize", totalUploadedSize, "io.ErrUnexpectedEOF", io.ErrUnexpectedEOF)
		}
	}

	// Loop over total uploaded parts to save them in
	// Parts array before completing the multipart request.
	for i := 1; i < partNumber; i++ {
		part, ok := partsInfo[i]
		if !ok {
			log.Fatalf("PartsInfo failed, missing part number %d", i)
		}
		complMultipartUpload.Parts = append(complMultipartUpload.Parts,
			minio.CompletePart{
				ETag:       part.ETag,
				PartNumber: part.PartNumber,
			})
	}

	// Sort all completed parts.
	sort.Sort(completedParts(complMultipartUpload.Parts))
	err = c.CompleteMultipartUpload(bucket, name, uploadID, complMultipartUpload.Parts)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Written %d bytes to %s in bucket %s.", totalUploadedSize, name, bucket)
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
