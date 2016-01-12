package xam

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"time"
)

// FileData represents attributes of file
type FileData struct {
	Path    string      `csv:"path"`
	Err     error       `csv:"err"`
	Size    int64       `csv:"size"`
	Mode    os.FileMode `csv:"mode"`
	SHA1    string      `csv:"sha1"`
	ModTime time.Time   `csv:"modified"`
}

// WalkFSTree passes each file encountered while walking tree into fileDataChan
// rootPath should be an absolute path
func WalkFSTree(fileDataChan chan FileData, rootPath string) {
	filepath.Walk(
		rootPath,
		func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				fileDataChan <- FileData{
					ModTime: info.ModTime(),
					Mode:    info.Mode(),
					Path:    path,
					Size:    info.Size(),
				}
			}
			return nil // Never stop
		})
}

// HashFile hashes a file with SHA1 and returns the hash as a byte slice
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

// HashFileHex hashes a file with SHA1 and returns the hash as a hex string
func HashFileHex(path string) (string, error) {
	h, err := HashFile(path)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h), nil
}

// ComputeHashes loops over file data from input,
// hashes each, and passes it down the output channel
func ComputeHashes(output chan FileData, input chan FileData) {
	for f := range input {
		h, err := HashFileHex(f.Path)
		f.SHA1 = h
		f.Err = err
		output <- f
	}
}
