// Copyright 2013 Thomas Emerson. All rights reserved.

// Package marc21 implements a MARC-21 reader.
package marc21

import (
	"errors"
	"io"
)

var (
	errInvalidLength = errors.New("marc21: record length is invalid")
	errNoRecordTerminator = errors.New("marc21: record must end in a RT")
)

const (
	delimiter = 0x1f
	fieldTerminator = 0x1e
	recordTerminator = 0x1d
)

const (
	leaderSize = 24
	maxRecordSize = 99999
)

func readRecord(r io.Reader) (int, []byte, error) {
	tmp := make([]byte, 5)

	_, e := r.Read(tmp)
	if e != nil {
		return 0, nil, e
	}

	rlen := decodeDecimal(tmp)
	if rlen < leaderSize + 2 || rlen > maxRecordSize {
		// (I think) the minimal size for a 'valid' record is the
		// size of the leader with a field terminator (ending the
		// directory) and the record terminator.
		// FIXME: check for maximum record size too!
		return 0, nil, errInvalidLength
	}

	result := make([]byte, rlen)
	copy(result, tmp)
	_, e = r.Read(result[5:])
	if e != nil {
		return 0, nil, e
	}

	if result[len(result) - 1] != recordTerminator {
		return 0, nil, errNoRecordTerminator
	}

	return rlen, result, nil
}

func decodeDecimal(n []byte) int {
	result := 0
	for i := range n {
		result = (10 * result) + int(n[i] - '0')
	}
	return result
}
