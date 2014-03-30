package index

import (
    "fmt"
    "encoding/json"
)

////////////////////////////////////////
// forward record, including document's arguments for filtering and sorting, 
// such as author, title, isbn, etc.
////////////////////////////////////////

type ForwardRecord struct {
    DocId   uint64
    Fields  map[string]string
}

func NewForwardRecord() *ForwardRecord {
    m := make(map[string]string)
    return &ForwardRecord{0, m}
}

// forward record parsing interface
type ForwardRecordParser interface {
    Parse(line string) (ForwardRecord, error)
}

type SimpleForwardRecordParser struct {}

func (this SimpleForwardRecordParser) Parse(line string) (ForwardRecord, error) {
    /*
        parse JSON-format forward record line
        "{"docid": 123456, "fields": {"key1": "value1", "key2": "value2"}}"
    */
    r := NewForwardRecord()
    err := json.Unmarshal([]byte(line), &r)
    if err != nil {
        fmt.Printf("%v\n", err)
        return *r, err
    }

    if r.DocId <= 0 {
        fmt.Printf("docid[%v] <=0\n", r.DocId)
        return *r, fmt.Errorf("docid[%v] <=0\n", r.DocId)
    }

    return *r, nil
}

////////////////////////////////////////


////////////////////////////////////////
// invert record for triggering, 
// such as term, category
////////////////////////////////////////

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

func NewInvertRecord() *InvertRecord {
    return &InvertRecord{}
}

// invert record parsing interface
type InvertRecordParser interface {
    Parse(line string) (InvertRecord, error)
}

type SimpleInvertRecordParser struct {}

func (this SimpleInvertRecordParser) Parse(line string) (InvertRecord, error) {
    /*
        parse JSON-format invert index line
        {
            "docid": 12345,     //attention: all input docid is outter docid
            "inverts": [
                {
                    "type": "term",         //invert type
                    "fields": [{
                        "k": "iphone",      //invert value
                        "v": "1.00"         //payload
                    }, {
                        "k": "5s",
                        "v": "0.001"
                    }]
                },{
                    "type": "category",
                    "fields": [{
                        "k": "111000",
                    }, {
                        "k": "113000",
                    }]
                }
            ]
        }
    */

    invertRecord := NewInvertRecord()

    err := json.Unmarshal([]byte(line), &invertRecord)
    if err != nil {
        fmt.Printf("json parse failed. %v [%s]\n", err, line)
        return *invertRecord, err
    }
    fmt.Printf("%v\n", invertRecord)
    return *invertRecord, nil
}
////////////////////////////////////////

