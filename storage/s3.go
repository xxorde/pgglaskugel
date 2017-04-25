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

package storage

import (
	"io"

	log "github.com/Sirupsen/logrus"
	minio "github.com/minio/minio-go"
	"github.com/spf13/viper"
)

func GetS3Connection() (minioClient minio.Client) {
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

	client.SetAppInfo(myName, Version)
	log.Debug("minioClient: ", minioClient)

	return *client
}

// writeStreamToS3 handles a stream and writes it to S3 storage
func WriteStreamToS3(input *io.Reader, bucket, name string) {
	location := viper.GetString("s3_location")
	encrypt := viper.GetBool("encrypt")
	contentType := "zstd"

	// Set contentType for encryption
	if encrypt {
		contentType = "pgp"
	}

	// Initialize minio client object.
	minioClient := getS3Connection()

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
