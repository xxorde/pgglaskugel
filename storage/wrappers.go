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
	"fmt"

	"github.com/spf13/viper"
	"github.com/xxorde/pgglaskugel/storage/backends/s3minioCs"

	s "github.com/xxorde/pgglaskugel/storageInterface"
)

var (
	// Definition in function below
	backends map[string]s.Backend
)

/*
	Not Interface functions below
*/

func init() {
	backends = initBackends()
}

// New creates a newly initialized storage backend
func New(viper *viper.Viper, storageType string) (backend s.Backend) {
	return backends[storageType].New(viper)
}

func initBackends() map[string]s.Backend {
	fbackends := make(map[string]s.Backend)

	var s3minioCs s3minioCs.S3backend
	//var file file.Localbackend
	fbackends["s3"] = s3minioCs
	//fbackends["file"] = file
	return fbackends
}

// CheckBackend checks if the configured backend is supported
func CheckBackend(backend string) error {
	if _, ok := backends[backend]; ok {
		return nil
	}
	return fmt.Errorf("Backend %s not supported", backend)
}
