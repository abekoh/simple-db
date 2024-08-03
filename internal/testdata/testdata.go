package testdata

import (
	"bufio"
	"embed"
	"fmt"
)

//go:embed *
var files embed.FS

func Iterator(filename string) (func(func(string) bool), error) {
	f, err := files.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("could not read file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	return func(fn func(string) bool) {
		for scanner.Scan() {
			if !fn(scanner.Text()) {
				break
			}
		}
	}, nil
}
