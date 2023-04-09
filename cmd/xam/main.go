package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"sync"

	"github.com/bradfitz/iter"
	"github.com/google/gops/agent"
	"github.com/hut8/xam"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var rootPath string
var dbPath string
var externChecksumPath string
var showLocal bool
var showRemote bool

func buildIndex(root, csvPath string) error {
	inputChan := make(chan xam.FileData)
	outputChan := make(chan xam.FileData)

	st, err := os.Stat(csvPath)
	exists := err == nil && st.Size() > 0
	if exists {
		log.Debugf("csv already exists: size %v", st.Size())
		entries, err := xam.ReadCSV(csvPath)
		if err != nil {
			log.Errorf("read existing csv failed: %v", err)
		}
		go func() {
			log.Debugf("loading %v existing entries", len(entries))
			for _, ent := range entries {
				outputChan <- *ent
			}
			log.Debugf("loaded %v existing entries", len(entries))
		}()
	}

	csvFile, err := os.OpenFile(csvPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer csvFile.Close()
	log.Debugf("using csv file: %v", csvFile.Name())

	wg := &sync.WaitGroup{}
	writeDone := make(chan struct{})
	go xam.WriteCSV(outputChan, csvFile, writeDone)

	workers := runtime.NumCPU()
	if workers > 3 {
		workers = 3
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

	return nil
}

func mainAction(c *cli.Context) error {
	csvPath, err := filepath.Abs(dbPath)
	if err != nil {
		panic(err)
	}
	log.Debugf("using csv path: %v", csvPath)

	root, err := filepath.Abs(rootPath)
	if err != nil {
		panic(err)
	}

	err = buildIndex(root, csvPath)
	if err != nil {
		log.WithError(err).Error("could not build db")
		return err
	}

	return err
}

func externDiffAction(c *cli.Context) error {
	localDB, err := xam.ReadCSV(dbPath)
	if err != nil {
		return err
	}
	remoteDB, ht, err := xam.ReadExtern(externChecksumPath)
	if err != nil {
		return err
	}
	if !(showLocal || showRemote) {
		showLocal = true
		showRemote = true
	}

	diffPaths := xam.ExternDiff(localDB, remoteDB, ht)
	if showLocal {
		for _, p := range diffPaths.LocalOnly {
			if showRemote {
				fmt.Printf("local: ")
			}
			fmt.Println(p)
		}
	}

	if showRemote {
		for _, p := range diffPaths.RemoteOnly {
			if showRemote {
				fmt.Printf("remote: ")
			}
			fmt.Println(p)
		}
	}
	return nil
}

func main() {
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stderr)
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Warnf("could not start gops agent: %v", err)
	}
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
			Usage:       "Path to CSV file",
			Destination: &dbPath,
			Required:    true,
		},
	}
	app.Commands = []cli.Command{
		{
			Name:   "extern-diff",
			Action: externDiffAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "extern-checksum",
					Usage:       "Path to external checksum file",
					Destination: &externChecksumPath,
					Required:    true,
				},
				cli.BoolFlag{
					Name:        "remote",
					Usage:       "Display files present only on remote",
					Destination: &showRemote,
					Required:    false,
				},
				cli.BoolFlag{
					Name:        "local",
					Usage:       "Display files present only locally",
					Destination: &showLocal,
					Required:    false,
				},
			},
		},
	}
	app.Run(os.Args)
}
