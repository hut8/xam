package main

import (
	"os"
	"path/filepath"
	"runtime"

	"sync"

	"io/ioutil"

	log "github.com/Sirupsen/logrus"
	"github.com/bradfitz/iter"
	"github.com/codegangsta/cli"
	"github.com/hut8/xam"
)

func makeCSVPath(root string) string {
	return filepath.Join(root, "xam.csv")
}

func buildFileSetIndex(root string) (string, error) {
	// inputChan receives files via WalkFSTree
	inputChan := make(chan xam.FileData)
	// reading from outputChan will give you the same as inputChan but with hashes
	outputChan := make(chan xam.FileData)

	index := xam.NewIndex()
	go index.FromFileData(outputChan)

	wg := &sync.WaitGroup{}
	for _ = range iter.N(runtime.NumCPU()) {
		wg.Add(1)
		go func() {
			xam.ComputeHashes(outputChan, inputChan)
			wg.Done()
		}()
	}

	xam.WalkFSTree(inputChan, root)

	close(inputChan) // notify hashers
	wg.Wait()

	close(outputChan) // notify index

	setSink, err := ioutil.TempFile(root, ".xam")
	if err != nil {
		return "", err
	}
	defer setSink.Close()
	log.Debugf("using temp file: %v", setSink.Name())
	index.Write(setSink)

	return setSink.Name(), nil
}

func buildIndex(root string) (string, error) {
	inputChan := make(chan xam.FileData)
	outputChan := make(chan xam.FileData)
	writeDoneChan := make(chan struct{})

	csvFile, err := ioutil.TempFile(root, ".xam")
	if err != nil {
		return "", err
	}
	defer csvFile.Close()
	log.Debugf("using temp csv: %v", csvFile.Name())

	go xam.WriteCSV(outputChan, csvFile, writeDoneChan)

	wg := &sync.WaitGroup{}
	for _ = range iter.N(runtime.NumCPU()) {
		wg.Add(1)
		go func() {
			xam.ComputeHashes(
				outputChan,
				inputChan)
			wg.Done()
		}()
	}

	xam.WalkFSTree(inputChan, root)

	close(inputChan) // notify hashers
	wg.Wait()

	close(outputChan) // notify csvwriter
	<-writeDoneChan

	return csvFile.Name(), nil
}

func mainAction(c *cli.Context) error {
	root := os.Getenv("TRACK_ROOT")
	if root == "" {
		root, _ = os.Getwd()
	}
	root, _ = filepath.Abs(root)
	csvPath := makeCSVPath(root)
	log.Debugf("using csv path: %v", csvPath)

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
	app := cli.NewApp()
	app.Name = "XAM"
	app.Usage = "Generate file indexes"
	app.Action = mainAction
	app.Commands = []cli.Command{
		cli.Command{
			Name: "index",
			Action: func(c *cli.Context) error {
				root, _ := filepath.Abs(".")
				indexPath := filepath.Join(root, "xam.ix")
				log.Infof("generating index to %v", indexPath)
				tmpPath, err := buildFileSetIndex(root)
				if err != nil {
					log.WithError(err).Error("could not build index")
					return err
				}
				err = os.Rename(tmpPath, indexPath)
				if err != nil {
					log.WithError(err).Error("could not move temp index to final destination")
					return err
				}
				return nil
			},
		},
	}
	app.Run(os.Args)
}
