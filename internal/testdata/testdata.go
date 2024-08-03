package testdata

import (
	"bufio"
	"embed"
	"strings"
)

//go:embed *.sql
var embedFiles embed.FS

func SQLIterator(filenames ...string) func(func(string, error) bool) {
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
				text := scanner.Text()
				if strings.HasPrefix(text, "--") {
					continue
				}
				if !yield(text, nil) {
					_ = f.Close()
					return
				}
			}
			_ = f.Close()
		}
	}
}
