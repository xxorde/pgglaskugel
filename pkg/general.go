// Copyright © 2016 Alexander Sosna <alexander@xxor.de>
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
package pkg

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/siddontang/go/log"
)

func AnswerConfirmation() (confirmed bool, err error) {
	var input string
	_, err = fmt.Scanln(&input)
	if err != nil {
		return false, err
	}
	positive := []string{"y", "yes", "do it", "let's rock"}
	negative := []string{"n", "no", "hell no", "fuck off"}

	input = strings.ToLower(input)

	for _, element := range positive {
		if element == input {
			return true, nil
		}
	}

	for _, element := range negative {
		if element == input {
			return false, nil
		}
	}
	doesNotParse := errors.New("Answer can not be parsed: " + input)
	return false, doesNotParse
}

func MustAnswerConfirmation() (confirmed bool) {
	if confirmed, err := AnswerConfirmation(); err == nil {
		return confirmed
	}
	return MustAnswerConfirmation()
}

func WatchOutput(input io.Reader, outputFunc func(args ...interface{})) {
	log.Debug("watchOutput started")
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		outputFunc(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		outputFunc("reading standard input:", err)
	}
	log.Debug("watchOutput end")
}
