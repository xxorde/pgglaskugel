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
