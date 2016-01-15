package main

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	log "github.com/Sirupsen/logrus"
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

func readCSV(csvPath string) ([]*xam.FileData, error) {
	csvFile, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	fileData := []*xam.FileData{}
	err = gocsv.UnmarshalFile(csvFile, fileData)
	if err != nil {
		return nil, err
	}
	return fileData, nil
}

func makeCSVPath(root string) string {
	return filepath.Join(root, "xam.csv")
}

func buildIndex(root string) error {
	inputChan := make(chan xam.FileData)
	outputChan := make(chan xam.FileData)

	csvFile, err := os.Create(makeCSVPath(root))
	if err != nil {
		return err
	}
	defer csvFile.Close()

	go writeCSV(outputChan, csvFile)

	for _ = range iter.N(runtime.NumCPU()) {
		go xam.ComputeHashes(outputChan, inputChan)
	}

	xam.WalkFSTree(inputChan, root)

	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "XAM"
	app.Usage = "Generate file indexes"
	app.Action = func(c *cli.Context) {
		root := os.Getenv("TRACK_ROOT")
		if root == "" {
			root, _ = os.Getwd()
		}
		root, _ = filepath.Abs(root)

		// Read existing CSV if any
		fileData, err := readCSV(
			makeCSVPath(root))
		if err != nil {
			log.Warnf("could not read existing database from: %s",
				root)
		}
		err = buildIndex(root)
		if err != nil {
			panic(err)
		}
	}
	app.Run(os.Args)
}
