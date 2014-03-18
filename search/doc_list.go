package search

import (
    "fmt"
    "sort"
)

type DocListIterator interface {
    Curr() (int, error)
    Next() (int, error)
    Find(docid int) (error)
}

type ArrayList struct {
    docIds  []int
    curPos  int
}

func NewArrayList(initSize int) (*ArrayList, error) {
    if initSize <= 0 {
        return nil, fmt.Errorf("initSize[%d] <= 0", initSize)
    }
    return &ArrayList{make([]int, initSize), 0}, nil
}

func (this *ArrayList) Curr() (curDocId int, err error) {
    if this.curPos >= 0 && len(this.docIds) < this.curPos {
        return this.docIds[this.curPos], nil
    } else {
        return -1, fmt.Errorf("out of bound. len[%d], pos[%d]", len(this.docIds), this.curPos)
    }
}

func (this *ArrayList) Next() (nextDocId int, err error) {
    this.curPos += 1
    if this.curPos >= 0 && len(this.docIds) < this.curPos {
        return this.docIds[this.curPos], nil
    } else {
        return -1, fmt.Errorf("out of bound. len[%d], pos[%d]", len(this.docIds), this.curPos)
    }
}

func (this *ArrayList) Find(docid int) (err error) {
    subLen := len(this.docIds) - this.curPos
    searchRes := sort.SearchInts(this.docIds[this.curPos:], docid)
    if searchRes < subLen {
        return nil
    } else {
        return fmt.Errorf("can not find docid[%d]", docid)
    }
}
