// Package backup - backup module
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
package backup

import (
	"errors"
	"regexp"

	log "github.com/Sirupsen/logrus"
)

// IsSane returns true if the backup seams sane
func (b *Backup) IsSane() (sane bool) {
	if b.Size < SaneBackupMinSize {
		return false
	}

	return true
}

// ParseBackupLabel checks the backup name
func ParseBackupLabel(b *Backup, backupLabel []byte) (backup *Backup, err error) {
	regStartWalLine := regexp.MustCompile(`^START WAL LOCATION: .*\/.* \(file [0-9A-Fa-f]{24}\)`)
	regStartWal := regexp.MustCompile(`[0-9A-Fa-f]{24}`)

	startWalLine := regStartWalLine.Find(backupLabel)
	if len(startWalLine) < 1 {
		log.Debug(string(backupLabel))
		return nil, errors.New("Can not find line with START WAL LOCATION")
	}

	startWal := regStartWal.Find(startWalLine)
	if len(startWal) < 1 {
		return nil, errors.New("Can not find START WAL")
	}

	b.StartWalLocation = string(startWal)
	return b, nil
}
