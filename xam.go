package xam

import (
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hut8/gocsv"
)

// WriteCSV serializeds xam.FileData instances to be read by ReadCSV
func WriteCSV(
	fileData chan FileData,
	csvFile io.Writer,
	doneChan chan struct{}) {
	w := csv.NewWriter(csvFile)
	w.Write([]string{
		"path", "modified", "size", "mode", "sha1", "error",
	})

	for d := range fileData {
		errorStr := ""
		if d.Err != nil {
			errorStr = d.Err.Error()
		}
		w.Write([]string{
			d.Path,
			strconv.FormatInt(d.ModTime.Time.UTC().Unix(), 10),
			strconv.FormatInt(d.Size, 10),
			d.Mode.String(),
			d.SHA1,
			errorStr,
		})
	}
	w.Flush()
	doneChan <- struct{}{}
}

// ReadCSV returns entries serialized by WriteCSV
func ReadCSV(csvPath string) ([]*FileData, error) {
	csvFile, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	fileData := []*FileData{}
	err = gocsv.UnmarshalFile(csvFile, &fileData)
	if err != nil {
		return nil, err
	}
	return fileData, nil
}

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
	Mode    os.FileMode `csv:"-"`
	SHA1    string      `csv:"sha1"`
	ModTime Time        `csv:"modified"`
}

type Time struct {
	time.Time
}

// MarshalCSV is used by github.com/gocarina/gocsv
func (t *Time) MarshalCSV() (string, error) {
	return strconv.FormatInt(
		t.UTC().Unix(), 10), nil
}

// UnmarshalCSV is used by github.com/gocarina/gocsv
func (t *Time) UnmarshalCSV(csv string) (err error) {
	u, err := strconv.ParseInt(csv, 10, 64)
	if err != nil {
		return err
	}
	t.Time = time.Unix(u, 0)
	return nil
}

// WalkFSTree passes each file encountered while walking tree into fileDataChan
// rootPath should be an absolute path
func WalkFSTree(fileDataChan chan FileData, rootPath string) {
	filepath.Walk(
		rootPath,
		func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				fileDataChan <- FileData{
					ModTime: Time{info.ModTime()},
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
