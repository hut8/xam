package main

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/bradfitz/iter"
	"github.com/codegangsta/cli"
	"github.com/hut8/gocsv"
	"github.com/hut8/xam"
)

func writeCSV(fileData chan xam.FileData,
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

func readCSV(csvPath string) ([]*xam.FileData, error) {
	csvFile, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	fileData := []*xam.FileData{}
	err = gocsv.UnmarshalFile(csvFile, &fileData)
	if err != nil {
		return nil, err
	}
	return fileData, nil
}

func makeCSVPath(root string) string {
	return filepath.Join(root, "xam.csv")
}

func makeHashCache(db *xam.FileDB) func(*xam.FileData) string {
	return func(fd *xam.FileData) string {
		return ""
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

	go writeCSV(outputChan, csvFile, writeDoneChan)

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
		fileData, err := readCSV(makeCSVPath(root))
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
