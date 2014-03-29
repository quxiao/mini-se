package index

import (
    "testing"
)

func TestForwardRecordNil(t *testing.T) {
    line := ``
    parser := MiniIndexParser{}
    forwardParser := SimpleForwardRecordParser{}
    _, err := parser.ParseForwardRecord(forwardParser, line)
    if err == nil {
        t.Errorf("shoud be not err!")
    }
}

func TestForwardRecordEmptyJson(t *testing.T) {
    line := `{}`
    parser := MiniIndexParser{}
    forwardParser := SimpleForwardRecordParser{}
    _, err := parser.ParseForwardRecord(forwardParser, line)
    if err == nil {
        t.Errorf("shoud be not err!")
    }
}

func TestForwardRecordNormal1(t *testing.T) {
    line := `{"docid": 123456, "fields": {"key1": "value1", "key2": "value2"}}`
    parser := MiniIndexParser{}
    forwardParser := SimpleForwardRecordParser{}
    r, err := parser.ParseForwardRecord(forwardParser, line)
    if err != nil {
        t.Errorf("error! %v", err)
    }
    if r.DocId <= 0 {
        t.Errorf("docid <= 0")
    }

    {
        v, err := r.Fields["key1"]
        if !err || v != "value1" {
            t.Errorf("parse error")
        }
    }

    {
        v, err := r.Fields["key2"]
        if !err || v != "value2" {
            t.Errorf("parse error")
        }
    }
}

func TestParseInvertRecord(t *testing.T) {
    line := `
        {
            "docid": 12345,
            "inverts": [
                {
                    "type": "term", 
                    "fields": [{
                        "k": "iphone",
                        "v": "1.00"
                    }, {
                        "k": "5s",
                        "v": "0.001"
                    }]
                },{
                    "type": "category",
                    "fields": [{
                        "k": "111000"
                    }, {
                        "k": "113000"
                    }]
                }
            ]
        }
    `
    parser := MiniIndexParser{}
    invertParser := SimpleInvertRecordParser{}
    invertRecord, err := parser.ParseInvertRecord(invertParser, line)
    if err != nil {
        t.Errorf("parse invert record failed")
    }
    if invertRecord.DocId != 12345 {
        t.Errorf("parse docid failed. [%d]", invertRecord.DocId)
    }
    if len(invertRecord.Inverts) != 2 {     //term and category
        t.Errorf("len[%d] != 2", len(invertRecord.Inverts))
    }
}
