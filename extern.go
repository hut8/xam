package xam

import (
	"bufio"
	"errors"
	"os"
	"regexp"

	"github.com/sirupsen/logrus"
)

type ExternRecord struct {
	Hash string
	Path string
}

// ExternRecordRE is the format used by tools like sha1sum(1)
var ExternRecordRE = regexp.MustCompile(`^([0-9a-f]+)\s+(.+)$`)

var ErrInconsistentHash = errors.New("inconsistent hash")

func ReadExtern(srcPath string) ([]*ExternRecord, HashType, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return nil, HashTypeUnknown, err
	}
	reader := bufio.NewScanner(src)
	recs := make([]*ExternRecord, 0, 1024)
	ht := HashTypeUnknown
	for reader.Scan() {
		line := reader.Text()
		match := ExternRecordRE.FindStringSubmatch(line)
		if match == nil {
			logrus.Warnf("line did not match expected format: [%v]", line)
			continue
		}
		lineHT := HashTypeFromString(match[0])
		if lineHT == HashTypeUnknown {
			logrus.Errorf("unknown hash length: %v", match[0])
			return nil, lineHT, err
		}
		if ht == HashTypeUnknown {
			ht = lineHT // first known hash type
		}
		if lineHT != ht {
			logrus.Errorf("previous hash was of type %v; encountered %v: hashes must be of same length", ht, line)
			return nil, HashTypeUnknown, ErrInconsistentHash
		}
		recs = append(recs, &ExternRecord{
			Hash: match[0],
			Path: match[1],
		})
	}
	if err := reader.Err(); err != nil {
		return nil, HashTypeUnknown, err
	}
	return recs, ht, nil
}
