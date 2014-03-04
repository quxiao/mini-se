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

func (parser *Parser) ParseInvertRecord(line string) (InvertIndex, bool) {
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
    var invert_record InvertRecord
    invert_index := make(InvertIndex)

    err := json.Unmarshal([]byte(line), &invert_record)
    if err != nil {
        fmt.Printf("json parse failed. %v [%s]\n", err, line)
        return invert_index, false
    }
    fmt.Printf("%v\n", invert_record)

    for _, invert := range invert_record.Inverts {
        fmt.Printf("%v\n", invert)
        for _, kv := range invert.Fields {
            fmt.Printf("k: [%s] -> v: [%s]\n", kv.K, kv.V)
            term := kv.K
            payload := kv.V
            //make uint64 sign
            h := fnv.New64()
            io.WriteString(h, invert.Type + "_" + term)     //term_iphone
            term_sign := TermSign(h.Sum64())
            fmt.Printf("term_sign: %d\n", term_sign)
            //push back to invert list
            invert_node := InvertNode{invert_record.DocId, payload}
            invert_list, ok := invert_index[term_sign]
            if !ok {
                var new_invert_list InvertList
                new_invert_list.Term = term
                new_invert_list.Type = invert.Type
                invert_list = new_invert_list
            }
            invert_list.InvertNodes = append(invert_list.InvertNodes, invert_node)
            invert_index[term_sign] = invert_list
        }
    }

    fmt.Printf("invert_index: %v\n", invert_index)
    return invert_index, ok
}
