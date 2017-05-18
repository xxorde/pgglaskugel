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

package util

import log "github.com/Sirupsen/logrus"

//Check function is a nice coating of indirection for error handling and logging
func Check(err error) error {
	return CheckFatal(err)
}

// CheckCustom calls and returns CheckFatalCustom
func CheckCustom(err error, output string) error {
	return CheckFatalCustom(err, output)
}

// CheckFatal calls and returns CheckFatalCustom
func CheckFatal(err error) error {
	return CheckFatalCustom(err, "")
}

// CheckError calls and returns CheckErrorCustom
func CheckError(err error) error {
	return CheckErrorCustom(err, "")
}

// CheckFatalCustom logs output/error and returns error, if one is given
func CheckFatalCustom(err error, output string) error {
	if err != nil {
		log.Fatal(output, err)
		return err
	}
	return nil
}

// CheckErrorCustom logs output/error and returns error, if one is given
func CheckErrorCustom(err error, output string) error {
	if err != nil {
		log.Error(output, err)
		return err
	}
	return nil
}
