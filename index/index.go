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
type InvertList struct {
    Term    string
    Type    string
    InvertNodes  []InvertNode
}

type InvertIndex map[TermSign]InvertList

