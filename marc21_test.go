// Copyright 2013 Thomas Emerson. All rights reserved.

package marc21

import (
	"bytes"
	"strings"
	"testing"
)

const (
	recordData = "00034 the string needs to be long" + string(recordTerminator)
	recordLength = len(recordData)
)

func TestReadRecord(t *testing.T) {
	d := strings.NewReader(recordData)

	n, rec, e := readRecord(d)
	if e != nil {
		t.Fatalf("Unable to read record: %v", e)
	}
	if n != recordLength {
		t.Errorf("Returned record size should be %d, got %v", recordLength, n)
	}		
	if !bytes.Equal([]byte(recordData), rec) {
		t.Errorf("Read data does not equal source data: %v", rec)
	}
}

func TestDecodeDecimal(t *testing.T) {
	if v := decodeDecimal([]byte("03245")); v != 3245 {
		t.Errorf("Conversion of \"03245\" did not equal 3245, rather %v", v);
	}

	if v := decodeDecimal([]byte("0")); v != 0 {
		t.Errorf("Conversion of \"0\" did not equal 0, rather %v", v);
	}
}
