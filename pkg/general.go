package pkg

import (
	"errors"
	"fmt"
	"strings"
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
