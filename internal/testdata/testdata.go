package testdata

import (
	"bufio"
	"embed"
)

//go:embed *
var embedFiles embed.FS

func Iterator(filenames ...string) func(func(string, error) bool) {
	return func(yield func(string, error) bool) {
		for _, filename := range filenames {
			f, err := embedFiles.Open(filename)
			if err != nil {
				if !yield("", err) {
					return
				}
				continue
			}
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				if !yield(scanner.Text(), nil) {
					_ = f.Close()
					return
				}
			}
			_ = f.Close()
		}
	}
}