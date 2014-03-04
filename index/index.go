package index

type SingleInvertNode struct {
    Term    string
    Docid   uint64
    Playload    string
}

type InvertNode struct {
    DocId   uint64
    Payload string
    //Weight  float64
    //Offset  uint32
}

type TermSign uint64
type InvertRecord struct {
    Term    string
    Type    uint32
    InvertList  []InvertNode
}

type InvertIndex map[TermSign]InvertRecord

