package index

import (
    "sync"
)

type InvertNode struct {
    DocId   uint64
    Payload string
    //Weight  float64
    //Offset  uint32
}

type TermSign uint64
type InvertList struct {
    Term    string
    Type    string
    InvertNodes  []InvertNode
}

type RawIndex map[TermSign]InvertList

type DocumentNode struct {
    Term    string
    Type    string
    Payload string
}

//MiniIndex wraps all interface for indexing
type MiniIndex struct {
    Initialized         bool
    TotalDocumentNum    uint64
    TotalTermNum        uint64
    innerRawIndex       RawIndex
    rwLock              *sync.RWMutex
    //TODO deleteDocSet        
}

func NewMiniIndex() *MiniIndex {
    return &MiniIndex{true, 0, 0, make(RawIndex), new(sync.RWMutex)}
}

func (this *MiniIndex)merge(i1 RawIndex, i2 RawIndex) RawIndex {
    mergedRawIndex := i1
    for k, v := range i2 {
        invertList, ok := mergedRawIndex[k]
        if ok {
            invertList.InvertNodes = append(invertList.InvertNodes, v.InvertNodes...)
            mergedRawIndex[k] = invertList
        } else {
            mergedRawIndex[k] = v
        }
    }
    return mergedRawIndex
}

func (this *MiniIndex)AddRawIndex(rawIndex RawIndex) error {
    this.rwLock.Lock()
    this.innerRawIndex = this.merge(this.innerRawIndex, rawIndex)
    this.rwLock.Unlock()
    return nil
}
