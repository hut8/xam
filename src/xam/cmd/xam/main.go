package main

import (
	"os"
	"path/filepath"
	"runtime"

	"sync"

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

func buildIndex(root string, hashCacheFunc xam.HashCacheFunc) error {
	inputChan := make(chan xam.FileData)
	outputChan := make(chan xam.FileData)
	writeDoneChan := make(chan struct{})

	csvFile, err := os.Create(makeCSVPath(root))
	if err != nil {
		return err
	}
	defer csvFile.Close()

	go xam.WriteCSV(outputChan, csvFile, writeDoneChan)

	wg := &sync.WaitGroup{}
a	for _ = range iter.N(runtime.NumCPU()) {
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
		fileData, err := xam.ReadCSV(makeCSVPath(root))
		if err != nil {
			log.WithError(err).Warnf(
				"could not read existing database from: %s",
				root)
		}
		fileDB := xam.NewFileDB(fileData)
		err = buildIndex(root,
			makeHashCache(fileDB))
		if err != nil {
			panic(err)
		}
	}
	app.Run(os.Args)
}
