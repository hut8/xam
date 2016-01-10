package xam

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

// FileData represents attributes of file
type FileData struct {
	os.FileInfo
	SHA1 string
	Path string // relative to root
	Err  error
}

// WalkFSTree passes each file encountered while walking tree into fileDataChan
// rootPath should be an absolute path
func WalkFSTree(fileDataChan chan FileData, rootPath string) {
	filepath.Walk(
		rootPath,
		func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				fileDataChan <- FileData{
					FileInfo: info,
					Path:     path,
				}
			}
			return nil // Never stop
		})
}

func HashFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	hasher := sha1.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return nil, err
	}

	return hasher.Sum(nil), nil
}

func HashFileHex(path string) (string, error) {
	h, err := hashFile(path)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h), nil
}

func ComputeHashes(output chan FileData, input chan FileData) {
	for f := range input {
		h, err := hashFileHex(f.Path)
		f.SHA1 = h
		f.Err = err
		output <- f
	}
}
