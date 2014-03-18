package index

import (
    "fmt"
    "encoding/json"
    "io"
    "hash/fnv"
)

type ForwardRecord struct {
    DocId   uint64
    Fields  map[string]string
}

func NewForwardRecord() *ForwardRecord {
    m := make(map[string]string)
    return &ForwardRecord{0, m}
}

type Parser struct {
    dirName    string
    fileName   string
}

func NewParser(dirName string, fileName string) *Parser {
    return &Parser{dirName, fileName}
}

func (parser *Parser) ParseForwardRecord(line string) (*ForwardRecord, bool) {
    /*
        parse JSON-format forward record line
        "{"docid": 123456, "fields": {"key1": "value1", "key2": "value2"}}"
    */

    ok := true
    r := NewForwardRecord()
    err := json.Unmarshal([]byte(line), &r)
    if err != nil {
        fmt.Printf("%v\n", err)
        ok = false
    }

    if r.DocId <= 0 {
        fmt.Printf("docid[%v] <=0\n", r.DocId)
        ok = false
    }

    fmt.Printf("%v, %v\n", r, ok)
    return r, ok
}

type KV struct {
    K   string
    V   string
}
type InvertRecordElement struct {
    Type    string
    Fields  []KV
}
type InvertRecord struct {
    DocId   uint64
    Inverts []InvertRecordElement
}

func (parser *Parser) ParseInvertRecord(line string) (RawIndex, bool) {
    /*
        parse JSON-format invert index line
        {
            "docid": 12345,
            "inverts": [
                {
                    "type": "term",      //term
                    "fields": [{
                        "k": "iphone",
                        "v": "1.00"
                    }, {
                        "k": "5s",
                        "v": "0.001"
                    }]
                },{
                    "type": "category",      //category
                    "fields": [{
                        "k": "111000",
                    }, {
                        "k": "113000",
                    }]
                }
            ]
        }
    */

    ok := true
    var invertRecord InvertRecord
    invertIndex := make(RawIndex)

    err := json.Unmarshal([]byte(line), &invertRecord)
    if err != nil {
        fmt.Printf("json parse failed. %v [%s]\n", err, line)
        return invertIndex, false
    }
    fmt.Printf("%v\n", invertRecord)

    for _, invert := range invertRecord.Inverts {
        fmt.Printf("%v\n", invert)
        for _, kv := range invert.Fields {
            fmt.Printf("k: [%s] -> v: [%s]\n", kv.K, kv.V)
            term := kv.K
            payload := kv.V
            //make uint64 sign
            h := fnv.New64()
            io.WriteString(h, invert.Type + "_" + term)     //term_iphone
            termSign := TermSign(h.Sum64())
            fmt.Printf("termSign: %d\n", termSign)
            //push back to invert list
            invertNode := InvertNode{invertRecord.DocId, payload}
            invertList, ok := invertIndex[termSign]
            if !ok {
                var newInvertList InvertList
                newInvertList.Term = term
                newInvertList.Type = invert.Type
                invertList = newInvertList
            }
            invertList.InvertNodes = append(invertList.InvertNodes, invertNode)
            invertIndex[termSign] = invertList
        }
    }

    fmt.Printf("invertIndex: %v\n", invertIndex)
    return invertIndex, ok
}
