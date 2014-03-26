package index

import (
    "sync"
    "fmt"
    "io"
    "hash/fnv"
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
    TotalDocumentNum    uint64      // max DocId, more or less
    TotalTermNum        uint64
    rawIndex            RawIndex
    forwardRecords      map[uint64]ForwardRecord
    rwLock              *sync.RWMutex
    deleteDocSet        map[uint64]bool
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
    for _, ire := range(ir.Inverts) {
        fmt.Println(ire)
        invertType := ire.Type
        for _, kv := range(ire.Fields) {
            term := kv.K
            payload := kv.V
            //make uint64 sign
            h := fnv.New64()
            io.WriteString(h, invertType + "_" + term)     //term_iphone
            termSign := TermSign(h.Sum64())
            fmt.Printf("termSign: %v\n", termSign)
            //push back to invert list
            invertNode := InvertNode{ir.DocId, payload}
            this.rwLock.Lock()
            invertList, ok := this.rawIndex[termSign]
            if !ok {
                var newInvertList InvertList
                newInvertList.Term = term
                newInvertList.Type = invertType
                invertList = newInvertList
            }
            invertList.InvertNodes = append(invertList.InvertNodes, invertNode)
            this.rawIndex[termSign] = invertList
            this.rwLock.Unlock()
        }
    }
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

func (this *MiniIndex) DeleteDocument(docId uint64) {
    this.rwLock.Lock()
    this.deleteDocSet[docId] = true
    this.rwLock.Unlock()
}

func (this *MiniIndex) DocumentExists(docId uint64) bool {
    this.rwLock.Lock()
    _, deleted := this.deleteDocSet[docId]
    this.rwLock.Unlock()
    return !deleted
}
