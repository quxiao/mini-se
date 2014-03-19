package index

import (
    "sync"
    "fmt"
)

type InvertNode struct {
    DocId   uint64
    Payload string      //strategy infomartion, including weight, offset, ect.
    //Weight  float64
    //Offset  uint32
}

type TermSign uint64        //usually keyword + type, such as "iphone_query", or "camera_category"

func (termSign TermSign) String() string {
    return fmt.Sprintf("[%016x]", uint64(termSign))
}

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
    rawIndex            RawIndex
    forwardRecords      map[uint64]ForwardRecord
    rwLock              *sync.RWMutex
    //TODO deleteDocSet        
}

func NewMiniIndex() *MiniIndex {
    return &MiniIndex{
        Initialized:        true,
        TotalDocumentNum:   0,
        TotalTermNum:       0,
        rawIndex:           make(RawIndex),
        forwardRecords:     make(map[uint64]ForwardRecord),
        rwLock:             new(sync.RWMutex)}
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

func (this *MiniIndex) AddRawIndex(rawIndex RawIndex) error {
    this.rwLock.Lock()
    this.rawIndex = this.merge(this.rawIndex, rawIndex)
    this.rwLock.Unlock()
    return nil
}

func (this *MiniIndex) AddInvertRecord(ir InvertRecord) error {
    //TODO
    return nil
}

func (this *MiniIndex) AddOrUpdateForwardRecord(fr ForwardRecord) error {
    curRecord, found := this.forwardRecords[fr.DocId]
    this.rwLock.Lock()
    if !found {
        this.forwardRecords[fr.DocId] = fr
    } else {
        for k, v := range fr.Fields {
            curRecord.Fields[k] = v
        }
    }
    this.rwLock.Unlock()
    return nil
}
