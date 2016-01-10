package main

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/bradfitz/iter"
	"github.com/hut8/xam"
)

func writeCSV(fileData chan xam.FileData, csvFile io.Writer) {
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
	inputChan := make(chan xam.FileData)
	outputChan := make(chan xam.FileData)

	csvFile, err := os.Create(
		filepath.Join(root, "xam.csv"))
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	go writeCSV(outputChan, csvFile)

	for _ = range iter.N(runtime.NumCPU()) {
		go xam.ComputeHashes(outputChan, inputChan)
	}

	xam.WalkFSTree(inputChan, root)
}
