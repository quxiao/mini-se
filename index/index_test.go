package index

import (
    "testing"
)

func TestAddInvertRecord_1(t *testing.T) {
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
    index := NewMiniIndex()
    invertParser := SimpleInvertRecordParser{}
    invertRecord, err := index.ParseInvertRecord(invertParser, line)
    if err != nil {
        t.Errorf("error! %v", err)
    }
    index.AddInvertRecord(invertRecord)
    docNum := index.stats.GetDocNum()
    if docNum != 1 {
        t.Errorf("docNum[%d] != 1", docNum)
    }
    signNum := index.stats.GetSignNum()
    if signNum != 4 {
        t.Errorf("signNum[%d] != 4", signNum)
    }
}
func TestAddInvertRecord_2(t *testing.T) {
    line1 := `
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
    line2 := `
        {
            "docid": 54321,
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
    index := NewMiniIndex()
    invertParser := SimpleInvertRecordParser{}
    invertRecord, err := index.ParseInvertRecord(invertParser, line1)
    if err != nil {
        t.Errorf("error! %v", err)
    }
    index.AddInvertRecord(invertRecord)
    invertRecord, err = index.ParseInvertRecord(invertParser, line2)
    if err != nil {
        t.Errorf("error! %v", err)
    }
    index.AddInvertRecord(invertRecord)
    docNum := index.stats.GetDocNum()
    if docNum != 2 {
        t.Errorf("docNum[%d] != 2", docNum)
    }
    signNum := index.stats.GetSignNum()
    if signNum != 4 {
        t.Errorf("signNum[%d] != 4", signNum)
    }
}

func TestAddOrUpdateForwardRecord(t *testing.T) {
    line := `{"docid": 123456, "fields": {"key1": "value1", "key2": "value2"}}`
    index := NewMiniIndex()
    forwardParser := SimpleForwardRecordParser{}
    r, err := index.ParseForwardRecord(forwardParser, line)
    if err != nil {
        t.Errorf("error! %v", err)
    }
    index.AddOrUpdateForwardRecord(r)
    docNum := index.stats.GetDocNum()
    if docNum != 1 {
        t.Errorf("docNum[%d] != 1", docNum)
    }
}
