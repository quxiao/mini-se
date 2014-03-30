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

type TermSign uint64        //usually type + keyword, such as "query_iphone", or "category_camera"

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

type IndexStats struct {
    DocIdSet            map[uint64]bool
    TermSet             map[uint64]bool
    lock                sync.RWMutex
}

func NewIndexStats() IndexStats {
    return IndexStats{
        DocIdSet:   make(map[uint64]bool),
        TermSet:    make(map[uint64]bool),
        lock:       sync.RWMutex{},
    }
}

func (this *IndexStats) AddDocId(docId uint64) {
    this.lock.Lock()
    this.DocIdSet[docId] = true
    this.lock.Unlock()
}

func (this *IndexStats) AddTerm(term uint64) {
    this.lock.Lock()
    this.TermSet[term] = true
    this.lock.Unlock()
}

func (this *IndexStats) GetDocNum() int {
    this.lock.RLock()
    n := len(this.DocIdSet)
    this.lock.RUnlock()
    return n
}

func (this *IndexStats) GetTermNum() int {
    this.lock.RLock()
    n := len(this.TermSet)
    this.lock.RUnlock()
    return n
}

//MiniIndex wraps all interface for indexing
type MiniIndex struct {
    initialized         bool
    indexStats          IndexStats
    rawIndex            RawIndex
    forwardRecords      map[uint64]ForwardRecord
    rwLock              sync.RWMutex
    deleteDocSet        map[uint64]bool
}

func NewMiniIndex() *MiniIndex {
    return &MiniIndex{
        initialized:        true,
        indexStats:         NewIndexStats(),
        rawIndex:           make(RawIndex),
        forwardRecords:     make(map[uint64]ForwardRecord),
        rwLock:             sync.RWMutex{}}
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
    this.rwLock.RLock()
    curRecord, found := this.forwardRecords[fr.DocId]
    this.rwLock.RUnlock()

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
    this.rwLock.RLock()
    _, deleted := this.deleteDocSet[docId]
    this.rwLock.RUnlock()
    return !deleted
}

////////////////////////////////////
// Parse forward and invert record
////////////////////////////////////

func (this *MiniIndex) ParseForwardRecord(forwardParser ForwardRecordParser, line string) (*ForwardRecord, error) {
    return forwardParser.Parse(line)
}

func (this *MiniIndex) ParseInvertRecord(invertParser InvertRecordParser, line string) (*InvertRecord, error) {
    return invertParser.Parse(line)
}

////////////////////////////////////

