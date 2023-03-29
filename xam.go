package xam

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ansel1/merry"
	humanize "github.com/dustin/go-humanize"
	"github.com/hut8/gocsv"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

// WriteCSV serializeds xam.FileData instances to be read by ReadCSV
func WriteCSV(
	fileData chan FileData,
	csvFile io.Writer,
	done chan struct{}) {

	w := csv.NewWriter(csvFile)
	w.Write([]string{
		"path", "modified", "size", "mode", "sha1", "md5", "error",
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
			d.MD5,
			errorStr,
		})
	}
	w.Flush()

	close(done)
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
	Path       string      `csv:"path"` // valid UTF-8 for output
	PathNative string      `csv:"-"`    // original path to hash invalid UTF-8 paths
	Err        error       `csv:"err"`
	Size       int64       `csv:"size"`
	Mode       os.FileMode `csv:"-"`
	SHA1       string      `csv:"sha1"`
	MD5        string      `csv:"md5"`
	ModTime    Time        `csv:"modified"`
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

// fileInfoFilter returns true if it should be skipped
func fileInfoFilter(fi os.FileInfo) bool {
	return (fi == nil || fi.IsDir())
}

// WalkFSTree passes each file encountered while walking tree into fileDataChan
// rootPath should be an absolute path
func WalkFSTree(fileDataChan chan FileData, rootPath string) {
	count := int64(0)
	size := int64(0)
	start := time.Now()

	filepath.Walk(
		rootPath,
		func(path string, info os.FileInfo, err error) error {
			if fileInfoFilter(info) {
				return nil
			}
			relPath, relErr := filepath.Rel(rootPath, path)
			if relErr != nil {
				panic(relErr)
			}
			nativePath := relPath
			if !utf8.ValidString(nativePath) {
				logrus.Errorf(`invalid string in path: "%v" bytes: %v`,
					path, hex.EncodeToString([]byte(nativePath)))
				err = fmt.Errorf(`invalid string in path: "%v" bytes: %v`,
					nativePath, hex.EncodeToString([]byte(nativePath)))
				relPath = strings.ToValidUTF8(relPath, "�")
			}
			fileDataChan <- FileData{
				ModTime:    Time{info.ModTime()},
				Mode:       info.Mode(),
				PathNative: nativePath,
				Path:       relPath,
				Size:       info.Size(),
				Err:        err,
			}
			count++
			size += info.Size()
			if count%10000 == 0 {
				elapsed := time.Since(start)
				byteRate := uint64(float64(size) / elapsed.Seconds())
				fileRate := uint64(float64(count) / elapsed.Seconds())
				logrus.Debugf("progress: hashed %v files\t%v\t%v/sec\t%v files/sec",
					humanize.Comma(count),
					humanize.Bytes(uint64(size)),
					humanize.Bytes(byteRate),
					humanize.Comma(int64(fileRate)),
				)
			}
			return nil // Never stop
		})
	logrus.Debugf("crawled %v files\t%v",
		humanize.Comma(count),
		humanize.Bytes(uint64(size)))
	close(fileDataChan)
}

// HashFile hashes a file with SHA1 and MD5 and returns the hashes as byte slices
func HashFile(path string) ([]byte, []byte, error) {
	alarm := time.NewTimer(10 * time.Second)
	defer alarm.Stop()

	fileChan := make(chan *os.File)
	errChan := make(chan error)

	go func() {
		f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
		if err != nil {
			errChan <- err
			return
		}
		fileChan <- f
	}()

	var f *os.File
	var err error

	select {
	case f = <-fileChan:
		defer f.Close()
	case err = <-errChan:
		logrus.Warn("failed to open %v: %v", path, err)
		return nil, nil, err
	case <-alarm.C:
		logrus.Error("timeout while opening %v", path)
		return nil, nil, merry.New("timeout")
	}

	sha1Hasher := sha1.New()
	md5Hasher := md5.New()
	sink := io.MultiWriter(sha1Hasher, md5Hasher)
	if _, err := io.Copy(sink, f); err != nil {
		return nil, nil, err
	}

	return sha1Hasher.Sum(nil), md5Hasher.Sum(nil), nil
}

// HashFileHex hashes a file with SHA1 and MD5 and returns the hashes as hex strings
func HashFileHex(path string) (string, string, error) {
	sha1Hash, md5Hash, err := HashFile(path)
	if err != nil {
		return "", "", err
	}
	return hex.EncodeToString(sha1Hash), hex.EncodeToString(md5Hash), nil
}

// HashCacheFunc allows hashes to be cached in a file.
// The function should return "" if it's unsure of the hash given the
// information in the FileData instance. The instance passed in will never
// have the SHA1 set to a non-zero value.
type HashCacheFunc func(*FileData) string

// ComputeHashes loops over file data from input,
// hashes each, and passes it down the output channel
func ComputeHashes(
	basePath string,
	output chan FileData,
	input chan FileData) {
	for f := range input {
		p := filepath.Join(basePath, f.PathNative)
		hashSha1, hashMD5, err := HashFileHex(p)
		f.SHA1 = hashSha1
		f.MD5 = hashMD5
		f.Err = err
		output <- f
	}
}
