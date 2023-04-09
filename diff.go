package xam

import (
	mapset "github.com/deckarep/golang-set/v2"
)

type DiffData struct {
	LocalOnly  []*FileData
	RemoteOnly []*FileData
}

func Diff(local, remote []*FileData) {

}

type DiffPaths struct {
	LocalOnly  []string
	RemoteOnly []string
}

func ExternDiff(local []*FileData, remote []*ExternRecord, ht HashType) *DiffPaths {
	localSet := mapset.NewSet[string]()
	for _, d := range local {
		localSet.Add(d.Hash(ht))
	}
	remoteSet := mapset.NewSet[string]()
	for _, d := range remote {
		remoteSet.Add(d.Hash)
	}

	localOnly := make([]string, 0, len(local))
	for _, localFD := range local {
		if !remoteSet.Contains(localFD.Hash(ht)) {
			localOnly = append(localOnly, localFD.Path)
		}
	}

	remoteOnly := make([]string, 0, len(remote))
	for _, remoteFD := range remote {
		if !localSet.Contains(remoteFD.Hash) {
			remoteOnly = append(remoteOnly, remoteFD.Path)
		}
	}
	return &DiffPaths{
		LocalOnly:  localOnly,
		RemoteOnly: remoteOnly,
	}
}
