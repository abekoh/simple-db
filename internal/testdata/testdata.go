package testdata

import (
	"bufio"
	"embed"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

//go:embed *
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

func CopySnapshotData(srcDirname, destDirPath string) error {
	files, err := embedFiles.ReadDir(path.Join("snapshots", srcDirname))
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}
	for _, file := range files {
		srcPath := path.Join("snapshots", srcDirname, file.Name())
		destPath := path.Join(destDirPath, file.Name())
		if file.IsDir() {
			continue
		}
		if err := copyFile(srcPath, destPath); err != nil {
			return fmt.Errorf("copy file: %w", err)
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	sourceFile, err := embedFiles.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}

	return nil
}
