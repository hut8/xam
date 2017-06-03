package xam

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// IndexEntry is a (Hash, Size) Tuple
// +gen set
type IndexEntry struct {
	Hash string
	Size int64
}

func NewIndexEntry(fd FileData) IndexEntry {
	return IndexEntry{
		Hash: fd.SHA1,
		Size: fd.Size,
	}
}

func (e *IndexEntry) String() string {
	return e.Hash + "-" + strconv.FormatInt(e.Size, 10)
}

type Index struct {
	Set     IndexEntrySet
	sizeSet map[int64]struct{}
}

func NewIndex() *Index {
	return &Index{
		Set:     NewIndexEntrySet(),
		sizeSet: make(map[int64]struct{}),
	}
}

func (i *Index) HasSize(sz int64) bool {
	_, ok := i.sizeSet[sz]
	return ok
}

func (i *Index) FromFileData(source chan FileData) {
	for fd := range source {
		entry := NewIndexEntry(fd)
		i.Set.Add(entry)
		i.sizeSet[entry.Size] = struct{}{}
	}
}

func (i *Index) LoadEntries(source chan IndexEntry) {
	for entry := range source {
		i.Set.Add(entry)
		i.sizeSet[entry.Size] = struct{}{}
	}
}

func (i *Index) LoadFromFile(path string) error {
	source, err := os.Open(path)
	if err != nil {
		return err
	}
	defer source.Close()

	// create a new scanner and read the file line by line
	scanner := bufio.NewScanner(source)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "-")
		if len(parts) != 2 {
			return fmt.Errorf("got malformed line: %+v", parts)
		}
		size, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return err
		}
		ent := IndexEntry{
			Hash: parts[0],
			Size: int64(size),
		}
		i.Set.Add(ent)
		i.sizeSet[ent.Size] = struct{}{}
	}
	return nil
}

func (i *Index) Write(w io.Writer) {
	for ent := range i.Set.Iter() {
		fmt.Fprintln(w, ent.String())
	}
}
