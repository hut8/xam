package xam

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// FileDB provides an in-memory database for querying duplicate
// files by size and SHA1
type FileDB struct {
	db FileDataSlice
}

// NewFileDB constructs a new FileDB instance
// given a slice of FileData instances
func NewFileDB(fd []*FileData) *FileDB {
	return &FileDB{
		db: FileDataSlice(fd),
	}
}

// FindBySize returns all files in FileDB with matching size
func (db *FileDB) FindBySize(size int64) []*FileData {
	return db.db.Where(func(fd *FileData) bool {
		return fd.Size == size
	})
}

// FindBySHA1 returns all files in FileDB with matching SHA1
func (db *FileDB) FindBySHA1(sha1 string) []*FileData {
	return db.db.Where(func(fd *FileData) bool {
		return fd.SHA1 == sha1
	})
}

// Add adds a new FileData object to the database if not already present
func (db *FileDB) Add(fd *FileData) {
	if len(db.db.Where(func(existingFD *FileData) bool {
		return existingFD == fd
	})) > 0 {
		return
	}
	db.db = append(db.db, fd)
}

// FileData represents attributes of file
// +gen * slice:"Where"
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
	fmt.Printf("root: %s\n", rootPath)
	filepath.Walk(
		rootPath,
		func(path string, info os.FileInfo, err error) error {
			fmt.Printf("found: %s\n", path)
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

// HashCacheFunc allows hashes to be cached in a file.
// The function should return "" if it's unsure of the hash given the
// information in the FileData instance. The instance passed in will never
// have the SHA1 set to a non-zero value.
type HashCacheFunc func(*FileData) string

// ComputeHashes loops over file data from input,
// hashes each, and passes it down the output channel
func ComputeHashes(output chan FileData,
	input chan FileData,
	hc HashCacheFunc) {
	for f := range input {
		cached := hc(&f)
		if cached == "" {
			h, err := HashFileHex(f.Path)
			f.SHA1 = h
			f.Err = err
		} else {
			f.SHA1 = cached
			f.Err = nil
		}
		output <- f
	}
}
