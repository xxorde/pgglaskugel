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

package s3minioCs

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"hash"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/backup"
	s "github.com/xxorde/pgglaskugel/storageInterface"
	"github.com/xxorde/pgglaskugel/util"

	log "github.com/Sirupsen/logrus"
	minio "github.com/minio/minio-go"
)

const (
	// maxPartsCount - maximum number of parts for a single multipart session.
	maxPartsCount = 10000

	// miniPartSize - minimum part size 64MiB per object after which
	// putObject behaves internally as multipart.
	//	minPartSize = 1024 * 1024 * 64

	// maxSinglePutObjectSize - maximum size 5TiB of object per PUT
	// operation.
	maxMultipartPutObjectSize = 1024 * 1024 * 1024 * 1024 * 5
)

var (
	extractTimeFromBackup = regexp.MustCompile(`.*@`) // Regexp to remove the name from a backup
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
	appName      string
	appVersion   string
	bucketBackup string
	bucketWal    string
	backupTo     string

	cmdZstd string
	cmdGpg  string

	endpoint        string
	accessKeyID     string
	secretAccessKey string
	location        string
	ssl             bool
	version         int
	encrypt         bool
	minPartSize     int64
	metadata        bool

	walName   string
	walTarget string
}

// New returns a newly initialized backend
func (b S3backend) New(viper *viper.Viper) (newBackend s.Backend) {
	b.bucketBackup = viper.GetString("s3_bucket_backup")
	b.bucketWal = viper.GetString("s3_bucket_wal")
	b.backupTo = viper.GetString("backup_to")

	b.cmdZstd = viper.GetString("path_to_zstd")
	b.cmdGpg = viper.GetString("path_to_gpg")

	b.endpoint = viper.GetString("s3_endpoint")
	b.accessKeyID = viper.GetString("s3_access_key")
	b.secretAccessKey = viper.GetString("s3_secret_key")
	b.location = viper.GetString("s3_location")
	b.ssl = viper.GetBool("s3_ssl")
	b.version = viper.GetInt("s3_protocol_version")
	b.encrypt = viper.GetBool("encrypt")
	b.minPartSize = int64(1024 * 1024 * viper.GetInt("s3_part_size_mb"))
	b.metadata = viper.GetBool("s3_metadata")

	b.walName = viper.GetString("walname")
	b.walTarget = viper.GetString("waltarget")
	return b
}

// GetWals returns WAL-Files from S3
func (b S3backend) GetWals() (a backup.Archive, err error) {
	log.Debug("Get backups from S3")

	// Initialize minio client object.
	minioClient := b.getS3Connection()

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	isRecursive := true
	objectCh := minioClient.ListObjects(b.bucketWal, "", isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return a, err
		}
		log.Debug(object)
		if object.Err != nil {
			return a, err
		}

		err = a.Add(object.Key, b.backupTo, object.Size)
		if err != nil {
			return a, err
		}

	}
	return a, nil
}

// GetBackups returns Backups
func (b S3backend) GetBackups() (backups backup.Backups) {
	log.Debug("Get backups from S3")
	// Initialize minio client object.
	minioClient := b.getS3Connection()
	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	isRecursive := true
	objectCh := minioClient.ListObjects(b.bucketBackup, "", isRecursive, doneCh)
	for object := range objectCh {
		var newBackup backup.Backup
		var err error
		if object.Err != nil {
			log.Error(object.Err)
		}
		log.Debug(object)

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
func (b S3backend) getS3Connection() (minioClient minio.Client) {
	var client *minio.Client
	var err error

	// Initialize minio client object.
	switch b.version {
	case 4:
		log.Debug("Using S3 version 4")
		client, err = minio.NewV4(b.endpoint, b.accessKeyID, b.secretAccessKey, b.ssl)
	case 2:
		log.Debug("Using S3 version 2")
		client, err = minio.NewV2(b.endpoint, b.accessKeyID, b.secretAccessKey, b.ssl)
	default:
		log.Debug("Autodetecting S3 version")
		client, err = minio.New(b.endpoint, b.accessKeyID, b.secretAccessKey, b.ssl)
	}
	if err != nil {
		log.Fatal(err)
	}

	client.SetAppInfo(b.appName, b.appVersion)
	log.Debug("minioClient: ", minioClient)

	return *client
}

// WriteStream handles a stream and writes it to S3 storage
func (b S3backend) WriteStream(input *io.Reader, name string, backuptype string) {
	var bucket string
	if backuptype == "basebackup" {
		bucket = b.bucketBackup
	} else if backuptype == "archive" {
		bucket = b.bucketWal
	} else {
		log.Fatalf(" unknown stream-type: %s\n", backuptype)
	}
	contentType := "zstd"

	// Set contentType for encryption
	if b.encrypt {
		contentType = "pgp"
	}

	// Create metadata for minio, can be disabled for not compatible storage
	var metaData map[string][]string
	if b.metadata {
		metaData = make(map[string][]string)
		metaData["Content-Type"] = []string{contentType}
		log.Debug("metaData: ", metaData)
	} else {
		log.Debug("Not using metadata")
	}

	// Initialize minio client object.
	minioClient := b.getS3Connection()

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		log.Debugf("Bucket already exists, we are using it: %s", bucket)
	} else {
		// Try to create bucket
		err = minioClient.MakeBucket(bucket, b.location)
		if err != nil {
			log.Debug("minioClient.MakeBucket(bucket, location) failed")
			log.Fatal(err)
		}
		log.Infof("Bucket %s created.", bucket)
	}

	// Get connection and set in minio.Core to use low level functions
	var c minio.Core
	c.Client = &minioClient

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
	totalPartsCount, partSize, _, err := optimalPartInfo(size, b.minPartSize)
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

func (b S3backend) readStream(name string, bucket string,
	output *io.Reader, start, wait chan bool) error {

	// Initialize minio client object.
	minioClient := b.getS3Connection()

	// Test if bucket is there
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		log.Fatal("Can not test for S3 bucket", err)
	}
	if !exists {
		log.Fatal("Bucket to fetch from does not exists,", bucket)
	}

	object, err := minioClient.GetObject(bucket, name)
	if err != nil {
		log.Error("Can not get object file from S3, ", name)
		log.Fatal(err)
	}
	defer object.Close()

	// Test if the object is accessible
	stat, err := object.Stat()
	if err != nil {
		log.Error("Can not get stats for object from S3, does object exists?")
		log.Fatal(err)
	}
	if stat.Size <= 0 {
		log.Fatal("Object has size <= 0")
	}

	var gpgStout io.ReadCloser
	if b.encrypt || stat.ContentType == "pgp" {
		log.Debug("content type: ", stat.ContentType)
		// We need to decrypt the wal file

		// Decrypt the compressed data
		gpgCmd := exec.Command(b.cmdGpg, "--decrypt", "-o", "-")
		// Set the decryption output as input for inflation
		gpgStout, err = gpgCmd.StdoutPipe()
		if err != nil {
			log.Fatal("Can not attach pipe to gpg process, ", err)
		}
		// Attach output of WAL to stdin
		gpgCmd.Stdin = object
		// Watch output on stderror
		gpgStderror, err := gpgCmd.StderrPipe()
		util.Check(err)
		go util.WatchOutput(gpgStderror, log.Warn, nil)

		// Start decryption
		if err := gpgCmd.Start(); err != nil {
			log.Fatal("gpg failed on startup, ", err)
		}
		log.Debug("gpg started")
	}

	// command to inflate the data stream
	inflateCmd := exec.Command(b.cmdZstd, "-d", "--stdout")

	// Expose output as output
	*output, err = inflateCmd.StdoutPipe()

	// Watch output on stderror
	inflateDone := make(chan struct{}) // Channel to wait for WatchOutput
	inflateStderror, err := inflateCmd.StderrPipe()
	util.Check(err)
	go util.WatchOutput(inflateStderror, log.Warn, inflateDone)

	// Assign inflationInput as Stdin for the inflate command
	if gpgStout != nil {
		// If gpgStout is defined use it as input
		inflateCmd.Stdin = gpgStout
	} else {
		// If gpgStout is not defined use raw WAL
		inflateCmd.Stdin = object
	}

	// Start inflation
	if err := inflateCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Debug("Inflation started")

	// Tell external listeners that we started
	if start != nil {
		start <- true
	}

	// Wait for external signal if chanel is set
	if wait != nil {
		<-wait
	}

	// Wait for watch goroutine before Cmd.Wait(), race condition!
	<-inflateDone

	// If there is still data in the output pipe it can be lost!
	err = inflateCmd.Wait()
	util.CheckCustom(err, "Inflation failed after startup, ")

	return err
}

// Fetch recover from a S3 compatible object store
func (b S3backend) Fetch() (err error) {
	walSource := b.walName + ".zst"

	// Open output file
	walFile, err := os.Create(b.walTarget)
	util.Check(err)
	defer walFile.Close()

	// Create writer for the walFile
	writer := bufio.NewWriter(walFile)

	// Create reader for the data stream
	var walStream io.Reader

	waitForStart := make(chan bool)
	tellToStop := make(chan bool)

	// Start to read file
	go b.readStream(walSource, b.bucketWal, &walStream, waitForStart, tellToStop)

	// Wait for readStream to start up before io.Copy
	<-waitForStart

	// Write walStream in walFile
	log.Debug("io.Copy(writer, walStream)")
	written, err := io.Copy(writer, walStream)
	writer.Flush()
	log.Debugf("io.Copy(writer, walStream) written: %d, err: %s", written, err)

	// Tell readStream to fish after io.Copy
	tellToStop <- true

	return err
}

// GetBasebackup gets things from S3
func (b S3backend) GetBasebackup(backup *backup.Backup,
	backupStream *io.Reader, wgStart *sync.WaitGroup, wgDone *sync.WaitGroup) {
	log.Debug("getFromS3")

	// Initialize minio client object.
	minioClient := b.getS3Connection()

	// Test if bucket is there
	exists, err := minioClient.BucketExists(b.bucketBackup)
	if err != nil {
		log.Fatal(err)
	}
	if !exists {
		log.Fatal("Bucket to restore from does not exists")
	}

	backupSource := backup.Name + backup.Extension
	backupObject, err := minioClient.GetObject(b.bucketBackup, backupSource)
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
	// Initialize minio client object.
	minioClient := b.getS3Connection()

	// We delete all backups, but start with the oldest just in case
	for i := len(backups.Backup) - 1; i >= 0; i-- {
		backup := backups.Backup[i]
		log.Debug("minioClient.RemoveObject(", b.bucketBackup, ", ", backup.Name+backup.Extension, ")")
		err = minioClient.RemoveObject(b.bucketBackup, backup.Name+backup.Extension)
		if err != nil {
			log.Warn("Error deleting backup: ", backup.Name+backup.Extension, " from ", b.bucketBackup, " err:", err)
		} else {
			count++
		}

	}
	return count, err

}

// GetStartWalLocation returns the oldest needed WAL file
// Every older WAL file is not required to use this backup
func (b S3backend) GetStartWalLocation(bp *backup.Backup) (startWalLocation string, err error) {
	// Initialize minio client object.
	minioClient := b.getS3Connection()
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
	objectCh := minioClient.ListObjects(b.bucketWal, "", isRecursive, doneCh)
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
			log.Debug(object.Key, " => seems to be a backup label, by size and name")

			// Create reader for the data stream
			var walStream io.Reader

			waitForStart := make(chan bool)
			tellToStop := make(chan bool)

			// Start to read file
			go b.readStream(object.Key, b.bucketWal, &walStream, waitForStart, tellToStop)

			// Wait for readStream to start up before io.Copy
			log.Debug("<-waitForStart")
			<-waitForStart

			// Create buffer write backup label to it
			backupLabel := util.StreamToByte(walStream)
			log.Debugf("backupLabel: %s,\nsize: %d", backupLabel, len(backupLabel))

			// Tell readStream to fish after io.Copy
			log.Debug("tellToStop <- true")
			tellToStop <- true

			// Find the correct backup label
			if len(regLabel.Find(backupLabel)) > 1 {
				log.Debug("Found matching backup label")
				err = bp.AddBackupLabel(backupLabel)
				if err != nil {
					log.Error("Failed to add backup label: ", err)
					log.Errorf("Backup label: %s", (backupLabel))
				}
				return bp.StartWalLocation, err
			}
		}
	}
	return "", errors.New("START WAL LOCATION not found")
}

// DeleteWal deletes the given WAL-file
func (b S3backend) DeleteWal(w *backup.Wal) (err error) {
	minioClient := b.getS3Connection()
	err = minioClient.RemoveObject(b.bucketWal, w.Name+w.Extension)
	if err != nil {
		log.Warn(err)
	}
	return err
}

// DeleteOldWal deletes all WAL files that are older than lastWalToKeep
func (b S3backend) DeleteOldWal(a *backup.Archive, lastWalToKeep backup.Wal) (deleted int) {
	// WAL files are deleted sequential
	// Due to the file system architecture parallel delete
	// Maybe this can be done in parallel for other storage systems
	visited := 0
	for _, wal := range a.WalFiles {
		// Count up
		visited++

		// Check if current visited WAL is older than lastWalToKeep
		// If it is older, delete it
		if wal.OlderThan(lastWalToKeep) {
			log.Debugf("Older than \"%s\" => going to delete: \"%s\"", lastWalToKeep.Name, wal.Name)
			err := b.DeleteWal(&wal)
			if err != nil {
				log.Warn(err)
				continue
			}
			deleted++
		}
	}
	log.Debugf("Checked %d files and deleted %d", visited, deleted)
	return deleted
}
