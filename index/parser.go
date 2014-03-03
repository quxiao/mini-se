package index

import (
    "fmt"
    "encoding/json"
)

type ForwardRecord struct {
    DocId   uint64
    Fields  map[string]string
}

type Parser struct {
    dirName    string
    fileName   string
}

func (parser *Parser) ParseForwardRecord(line string) (*ForwardRecord, bool) {
    /*
        parse json-format forward record line
        "{"docid": 123456, "fields": {"key1": "value1", "key2": "value2"}}"
    */

    ok := true
    r := new(ForwardRecord)
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
