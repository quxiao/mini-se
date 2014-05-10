package index

import (
    "sync"
    "fmt"
    "io"
    "hash/fnv"
)

//MiniIndex wraps all interface for indexing
type MiniIndex struct {
    initialized         bool
    stats               IndexStats
    rawIndex            RawIndex
    forwardRecords      map[uint64]ForwardRecord
    rwLock              sync.RWMutex
    deleteDocSet        map[uint64]bool
}

func NewMiniIndex() *MiniIndex {
    return &MiniIndex{
        initialized:        true,
        stats:              NewIndexStats(),
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
    defer this.rwLock.Unlock()
    this.rawIndex = this.merge(this.rawIndex, rawIndex)
    return nil
}

func (this *MiniIndex) AddInvertRecord(ir InvertRecord) error {
    this.stats.AddDocId(ir.DocId)
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
            this.stats.AddSign(termSign)
        }
    }
    return nil
}

func (this *MiniIndex) AddOrUpdateForwardRecord(fr ForwardRecord) error {
    this.rwLock.Lock()
    defer this.rwLock.Unlock()

    curRecord, found := this.forwardRecords[fr.DocId]
    if !found {
        this.forwardRecords[fr.DocId] = fr
    } else {
        for k, v := range fr.Fields {
            curRecord.Fields[k] = v
        }
    }
    this.stats.AddDocId(fr.DocId)
    delete(this.deleteDocSet, fr.DocId)

    return nil
}

func (this *MiniIndex) DeleteDocument(docId uint64) {
    this.rwLock.Lock()
    defer this.rwLock.Unlock()
    this.deleteDocSet[docId] = true
}

func (this *MiniIndex) DocumentExists(docId uint64) bool {
    this.rwLock.RLock()
    defer this.rwLock.RUnlock()
    _, deleted := this.deleteDocSet[docId]
    return !deleted
}

////////////////////////////////////
// Parse forward and invert record
////////////////////////////////////

func (this *MiniIndex) ParseForwardRecord(forwardParser ForwardRecordParser, line string) (ForwardRecord, error) {
    return forwardParser.Parse(line)
}

func (this *MiniIndex) ParseInvertRecord(invertParser InvertRecordParser, line string) (InvertRecord, error) {
    return invertParser.Parse(line)
}

////////////////////////////////////

