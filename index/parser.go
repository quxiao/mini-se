package index

import (
    "fmt"
    "encoding/json"
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

func (parser *Parser) ParseInvertRecord(line string) ([]SingleInvertNode, bool) {
    /*
        parse JSON-format invert index line
        {
            "docid": 12345,
            "inverts": [
                {
                    "type": 0,      //term
                    "fields": [{
                        "k": "iphone",
                        "v": "1.00"
                    }, {
                        "k": "5s",
                        "v": "0.001"
                    }]
                },{
                    "type": 1,      //category
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
    var single_invert_nodes []SingleInvertNode
    var inter interface{}

    err := json.Unmarshal([]byte(line), &inter)
    if err != nil {
        fmt.Printf("json parse failed. %v [%s]\n", err, line)
        return single_invert_nodes, false
    }
    res, ok := inter.(map[string]interface{})
    if !ok {
        fmt.Printf("json transform failed.\n")
        return single_invert_nodes, false
    }
    for k, v := range res {
        switch v.(type) {
        case float64:
            fmt.Printf("%v %v\n", k, v)
        }
    }

    return single_invert_nodes, ok
}
