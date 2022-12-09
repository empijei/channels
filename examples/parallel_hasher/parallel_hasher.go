package main

import (
	"encoding/base64"
	"fmt"
	"hash/adler32"
	"os"

	. "github.com/empijei/channels"
)

// TODO: scope object with makeChan, Error, Go, Wait funcs?

func Run() error {
	errs, err := ErrorScope(nil)

	lines := FromFileLines("testdata/input.txt", errs)()
	hashes := ParallelMap(0,
		func(in string) (hash string) {
			hr := adler32.New()
			return base64.StdEncoding.EncodeToString(hr.Sum([]byte(in)))
		})(lines)
	ToFileLines[string]("testdata/output.txt", errs, nil)(hashes)

	return EndScope(errs, err)
}

func main() {
	if err := Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
