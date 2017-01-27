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
