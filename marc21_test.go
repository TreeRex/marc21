// Copyright 2013 Thomas Emerson. All rights reserved.

package marc21

import (
	"bytes"
	"strings"
	"testing"
)

const (
	// this record extracted from the Harvard Library Open Metadata
	// http://openmetadata.lib.harvard.edu/bibdata
	fullRecord    = "00458nam a22001577u 4500001001200000005001700012008004100029035001600070245005400086260004100140300003500181650003100216710003300247988001300280906000700293\x1e000000002-7\x1e20120831093346.0\x1e821202|1937    |||||||  |||| |0||||eng|d\x1e0 \x1faocm83544809\x1e00\x1faGarden exhibition /\x1fcSan Francisco Museum of Art.\x1e0 \x1faSan Francisco :\x1fbThe Museum,\x1fc[1937]\x1e  \x1fa1 folded sheet (4p.) ;\x1fc14 cm.\x1e 0\x1faHorticultural exhibitions.\x1e2 \x1faSan Francisco Museum of Art.\x1e  \x1fa20020608\x1e  \x1f0MH\x1e\x1d"
	fullRecordLen = len(fullRecord)

	titleStatement = "00\x1faGarden exhibition /\x1fcSan Francisco Museum of Art.\x1e"
)

func TestReadRecord(t *testing.T) {
	d := strings.NewReader(fullRecord)

	n, rec, e := readRecord(d)
	if e != nil {
		t.Fatalf("Unable to read record: %v", e)
	}
	if n != fullRecordLen {
		t.Errorf("Returned record size should be %d, got %v", fullRecordLen, n)
	}
	if !bytes.Equal([]byte(fullRecord), rec) {
		t.Errorf("Read data does not equal source data: %v", rec)
	}
}

func TestDecodeDecimal(t *testing.T) {
	if v := decodeDecimal([]byte("03245")); v != 3245 {
		t.Errorf("Conversion of \"03245\" did not equal 3245, rather %v", v)
	}

	if v := decodeDecimal([]byte("0")); v != 0 {
		t.Errorf("Conversion of \"0\" did not equal 0, rather %v", v)
	}
}

func TestLeaderValidation(t *testing.T) {
	if !validLeader([]byte(fullRecord)) {
		t.Errorf("Valid leader did not pass validation")
	}
}

func TestDirectoryLoader(t *testing.T) {
	d := decodeDirectory([]byte(fullRecord))

	if len(d) != 11 {
		t.Errorf("Invalid entry count in directory")
	}
}

func TestRawFieldExtraction(t *testing.T) {
	m, _ := NewMarcRecord([]byte(fullRecord))

	field := m.GetRawField("245")
	if field.ValueCount() != 1 {
		t.Fatalf("More than one entry value returned")
	}

	if !bytes.Equal([]byte(titleStatement), field.GetRawValue(0)) {
		t.Errorf("Returned entry does not match raw data")
	}

	if field.IsControlField() {
		t.Errorf("Field 245 is not a control field")
	}

	// ask for a non-existent field
	field = m.GetRawField("666")
	if field.ValueCount() != 0 {
		t.Errorf("Non-existent field returns non-0 value count")
	}

	// get a control field
	field = m.GetRawField("001")
	if field.ValueCount() != 1 {
		t.Errorf("Required field \"001\" not found")
	}
	if !field.IsControlField() {
		t.Errorf("Field \"001\" not marked as a control field")
	}
}

func TestRawSubFieldExtraction(t *testing.T) {
	m, _ := NewMarcRecord([]byte(fullRecord))
	field := m.GetRawField("245")

	subfield := field.GetNthRawSubfield("a", 0)
	if subfield == nil {
		t.Errorf("Unable to get 245$a")
	}
	if string(subfield) != "Garden exhibition /" {
		t.Errorf("Value returned for 245$a is wrong: %v", string(subfield))
	}

	subfield = field.GetNthRawSubfield("z", 0)
	if subfield != nil {
		t.Errorf("Got a value for 245$z, which doesn't exist")
	}

	subfield = field.GetNthRawSubfield("c", 0)

	if subfield == nil {
		t.Errorf("Unable to get 245$c")
	}
	if string(subfield) != "San Francisco Museum of Art." {
		t.Errorf("Value returned for 245$c is wrong: %v", string(subfield))
	}
}

func TestSubFieldExtraction(t *testing.T) {
	m, _ := NewMarcRecord([]byte(fullRecord))
	field := m.GetRawField("245")

	subfield := field.GetNthSubfield("a", 0)
	if subfield == "" {
		t.Errorf("Unable to get 245$a")
	}
	if subfield != "Garden exhibition /" {
		t.Errorf("Value returned for 245$a is wrong: %v", subfield)
	}
}
