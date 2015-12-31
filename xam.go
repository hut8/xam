package main

import (
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/bradfitz/iter"
)

// FileData represents attributes of file
type FileData struct {
	os.FileInfo
	SHA1 string
	Path string // relative to root
	Err  error
}

// walkFSTree passes each file encountered while walking tree into fileDataChan
// rootPath should be an absolute path
func walkFSTree(fileDataChan chan FileData, rootPath string) {
	filepath.Walk(
		rootPath,
		func(path string, info os.FileInfo, err error) error {
			fileDataChan <- FileData{
				FileInfo: info,
				Path:     path,
			}
			return nil // Never stop
		})
}

func hashFile(path string) ([]byte, error) {
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

func hashFileHex(path string) (string, error) {
	h, err := hashFile(path)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h), nil
}

func computeHashes(output chan FileData, input chan FileData) {
	for f := range input {
		h, err := hashFileHex(f.Path)
		f.SHA1 = h
		f.Err = err
		output <- f
	}
}

func writeCSV(fileData chan FileData, csvFile io.Writer) {
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
			d.ModTime().String(),
			strconv.FormatInt(d.Size(), 10),
			d.Mode().String(),
			d.SHA1,
			errorStr,
		})
	}
}

func main() {
	root := os.Getenv("TRACK_ROOT")
	if root == "" {
		root, _ = os.Getwd()
	}
	root, _ = filepath.Abs(root)
	inputChan := make(chan FileData)
	outputChan := make(chan FileData)

	csvFile, err := os.Create(
		filepath.Join(root, "xam.csv"))
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	go writeCSV(outputChan, csvFile)

	for _ = range iter.N(runtime.NumCPU()) {
		go computeHashes(outputChan, inputChan)
	}

	walkFSTree(inputChan, root)
}
