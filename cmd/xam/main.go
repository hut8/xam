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

// This is obviously dysfunctional but works for me!
func makeHashCache(db *xam.FileDB) func(*xam.FileData) string {
	return func(fd *xam.FileData) string {
		matches := xam.FileDataSlice(db.FindBySize(fd.Size))
		if len(matches) == 0 {
			return ""
		}
		log.Debugf("using cached value for: %s",
			fd.Path)
		return matches[0].SHA1
	}
}

func buildIndex(root string, hashCacheFunc xam.HashCacheFunc) (string, error) {
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
				inputChan,
				hashCacheFunc)
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

	// Read existing CSV if any
	fileData, err := xam.ReadCSV(csvPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf(
				"existing database not found for %s",
				root)
		} else {
			log.WithError(err).Errorf(
				"could not read existing database from: %s",
				root)
		}
	}

	// Build current database
	fileDB := xam.NewFileDB(fileData)
	tmpCsvPath, err := buildIndex(root,
		makeHashCache(fileDB))
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
	app := cli.NewApp()
	app.Name = "XAM"
	app.Usage = "Generate file indexes"
	app.Action = mainAction
	app.Run(os.Args)
}
