package index

import (
    "testing"
)

func TestForwardRecordNil(t *testing.T) {
    line := ``
    parser := Parser{}
    _, ok := parser.ParseForwardRecord(line)
    if ok {
        t.Errorf("shoud be not ok!")
    }
}

func TestForwardRecordEmptyJson(t *testing.T) {
    line := `{}`
    parser := Parser{}
    _, ok := parser.ParseForwardRecord(line)
    if ok {
        t.Errorf("shoud be not ok!")
    }
}

func TestForwardRecordNormal1(t *testing.T) {
    line := `{"docid": 123456, "fields": {"key1": "value1", "key2": "value2"}}`
    parser := Parser{}
    r, ok := parser.ParseForwardRecord(line)
    if !ok {
        t.Errorf("error! %v", ok)
    }
    if r.DocId <= 0 {
        t.Errorf("docid <= 0")
    }

    {
        v, ok := r.Fields["key1"]
        if !ok || v != "value1" {
            t.Errorf("parse error")
        }
    }

    {
        v, ok := r.Fields["key2"]
        if !ok || v != "value2" {
            t.Errorf("parse error")
        }
    }
}

func TestInvertRecordNormal1(t *testing.T) {
    line := `
        {
            "docid": 12345,
            "inverts": [
                {
                    "type": 0, 
                    "fields": [{
                        "k": "iphone",
                        "v": "1.00"
                    }, {
                        "k": "5s",
                        "v": "0.001"
                    }]
                }
            ]
        }
    `

    parser := Parser{}
    _, ok := parser.ParseInvertRecord(line)
    if !ok {
        t.Errorf("parse failed")
    }
}
