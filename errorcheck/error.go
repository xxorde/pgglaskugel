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

package errorcheck

import "github.com/siddontang/go/log"

// Nice coating of indirection for error handling and logging
func Check(err error) error {
	return CheckFatal(err)
}

func CheckCustom(err error, output string) error {
	return CheckFatalCustom(err, output)
}

func CheckFatal(err error) error {
	return CheckFatalCustom(err, "")
}

func CheckError(err error) error {
	return CheckErrorCustom(err, "")
}

func CheckFatalCustom(err error, output string) error {
	if err != nil {
		log.Fatal(output, err)
		return err
	}
	return nil
}

func CheckErrorCustom(err error, output string) error {
	if err != nil {
		log.Error(output, err)
		return err
	}
	return nil
}
