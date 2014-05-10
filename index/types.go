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
    SignSet             map[TermSign]bool
    lock                sync.RWMutex
}

func NewIndexStats() IndexStats {
    return IndexStats{
        DocIdSet:   make(map[uint64]bool),
        SignSet:    make(map[TermSign]bool),
        lock:       sync.RWMutex{},
    }
}

func (this *IndexStats) AddDocId(docId uint64) {
    this.lock.Lock()
    defer this.lock.Unlock()
    this.DocIdSet[docId] = true
}

func (this *IndexStats) RemoveDocId(docId uint64) {
    this.lock.Lock()
    defer this.lock.Unlock()
    delete(this.DocIdSet, docId)
}

func (this *IndexStats) AddSign(sign TermSign) {
    this.lock.Lock()
    defer this.lock.Unlock()
    this.SignSet[sign] = true
}

func (this *IndexStats) RemoveSign(sign TermSign) {
    this.lock.Lock()
    defer this.lock.Unlock()
    delete(this.SignSet, sign)
}

func (this *IndexStats) GetDocNum() int {
    this.lock.RLock()
    defer this.lock.RUnlock()
    return len(this.DocIdSet)
}

func (this *IndexStats) GetSignNum() int {
    this.lock.RLock()
    defer this.lock.RUnlock()
    return len(this.SignSet)
}

