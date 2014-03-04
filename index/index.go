package index

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

type Index map[TermSign]InvertList

func Merge(i1 Index, i2 Index) Index {
    mergedIndex := make(Index)
    for k, v := range i1 {
        mergedIndex[k] = v
    }
    for k, v := range i2 {
        invertList, ok := mergedIndex[k]
        if ok {
            invertList.InvertNodes = append(invertList.InvertNodes, v.InvertNodes...)
            mergedIndex[k] = invertList
        } else {
            mergedIndex[k] = v
        }
    }

    return mergedIndex
}
