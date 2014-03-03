package index

import (
    "fmt"
    "testing"
)

func TestNil(t *testing.T) {
    line := ``
    parser := Parser{}
    _, ok := parser.ParseForwardRecord(line)
    if ok {
        t.Errorf("shoud be not ok!")
    }
}

func TestEmptyJson(t *testing.T) {
    line := `{}`
    parser := Parser{}
    _, ok := parser.ParseForwardRecord(line)
    if ok {
        t.Errorf("shoud be not ok!")
    }
}

func TestNormal1(t *testing.T) {
    line := `{"docid": 123456, "fields": {"key1": "value1", "key2": "value2"}}`
    parser := Parser{}
    r, ok := parser.ParseForwardRecord(line)
    if !ok {
        t.Errorf("error! %v", ok)
    }
    if r.DocId == 0 {
        t.Errorf("docid is 0")
    }
    fmt.Printf("record: %v\n", r)
}
