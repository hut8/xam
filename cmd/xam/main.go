package main

import (
	"os"
	"path/filepath"
	"runtime"

	"sync"

	"io/ioutil"

	"github.com/bradfitz/iter"
	"github.com/hut8/xam"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var rootPath string
var outputPath string

func buildIndex(root string) (string, error) {
	inputChan := make(chan xam.FileData)
	outputChan := make(chan xam.FileData)

	csvFile, err := ioutil.TempFile("", "xam-csv")
	if err != nil {
		return "", err
	}
	defer csvFile.Close()
	log.Debugf("using temp csv: %v", csvFile.Name())

	wg := &sync.WaitGroup{}
	writeDone := make(chan struct{})
	go xam.WriteCSV(outputChan, csvFile, writeDone)

	workers := runtime.NumCPU()
	if workers > 8 {
		workers = 8
	}
	for range iter.N(workers) {
		wg.Add(1)
		go func() {
			xam.ComputeHashes(
				root,
				outputChan,
				inputChan)
			wg.Done()
		}()
	}

	xam.WalkFSTree(inputChan, root)

	wg.Wait()         // wait on all hashers to stop
	close(outputChan) // stop csv writer
	<-writeDone       // wait for csv writer to complete

	return csvFile.Name(), nil
}

func mainAction(c *cli.Context) error {
	csvPath, err := filepath.Abs(outputPath)
	if err != nil {
		panic(err)
	}
	log.Debugf("using csv path: %v", csvPath)

	root, err := filepath.Abs(rootPath)
	if err != nil {
		panic(err)
	}

	tmpCsvPath, err := buildIndex(root)
	if err != nil {
		log.WithError(err).Error("could not build db")
		return err
	}

	// Atomically(?) move
	log.Debugf("moving %v -> %v", tmpCsvPath, csvPath)
	err = os.Rename(tmpCsvPath, csvPath)
	if err != nil {
		log.WithError(err).Error("could not move")
	}
	return err
}

func main() {
	log.SetLevel(log.DebugLevel)
	wd, _ := os.Getwd()

	app := cli.NewApp()
	app.Name = "XAM"
	app.Usage = "Generate file indexes"
	app.Action = mainAction
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "root",
			Usage:       "Path to root of files to index",
			Value:       wd,
			Destination: &rootPath,
		},
		cli.StringFlag{
			Name:        "out",
			Usage:       "Path to CSV file output",
			Destination: &outputPath,
			Required:    true,
		},
	}
	app.Run(os.Args)
}
