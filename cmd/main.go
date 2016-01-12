package main

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/bradfitz/iter"
	"github.com/codegangsta/cli"
	"github.com/gocarina/gocsv"
	"github.com/hut8/xam"
)

func writeCSV(fileData chan xam.FileData, csvFile io.Writer) error {
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
			d.ModTime.String(),
			strconv.FormatInt(d.Size, 10),
			d.Mode.String(),
			d.SHA1,
			errorStr,
		})
	}
	return nil
}

func readCSV(csvFile *os.File) ([]*xam.FileData, error) {
	fileData := []*xam.FileData{}
	err := gocsv.UnmarshalFile(csvFile, fileData)
	if err != nil {
		return nil, err
	}
	return fileData, nil
}

func buildIndex() {
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

func main() {
	app := cli.NewApp()
	app.Name = "XAM"
	app.Usage = "Generate file indexes"
	app.Action = func(c *cli.Context) {
		buildIndex()
	}
	app.Run(os.Args)
}
