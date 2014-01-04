// Copyright 2013 Thomas Emerson. All rights reserved.

// Package marc21 implements a MARC-21 reader.
package marc21

import (
	"errors"
	"io"
	"strings"
)

var (
	errInvalidLength      = errors.New("marc21: record length is invalid")
	errNoRecordTerminator = errors.New("marc21: record must end in a RT")
	errInvalidLeader      = errors.New("marc21: leader is invalid")
)

const (
	delimiter        = 0x1f
	fieldTerminator  = 0x1e
	recordTerminator = 0x1d
)

const (
	leaderSize    = 24
	maxRecordSize = 99999
)

type Entry struct {
	fieldLength    int
	startingOffset int
}

type VariableField struct {
	rawData [][]byte
}

type MarcRecord struct {
	RawRecord         []byte
	Status            byte
	Type              byte
	BibLevel          byte
	CharacterEncoding byte
	EncodingLevel     byte
	CatalogingForm    byte
	MultipartLevel    byte
	Directory         map[string][]Entry
}

// Array of valid values for positions in the MARC 21 leader
var marc21LeaderValues = []struct {
	offset int
	values string
}{
	{5, "acdnp"},
	{6, "acdefgijkmoprt"},
	{7, "abcdims"},
	{8, " a"},
	{9, " a"},
	{10, "2"},
	{11, "2"},
	// 12 -- 16 Base address of data
	{17, " 1234578uz"},
	{18, " aciu"},
	{19, " abc"},
	{20, "4"},
	{21, "5"},
	{22, "0"},
	{23, "0"},
}

func NewMarcRecord(rawData []byte) (*MarcRecord, error) {
	// this assumes that rawData is a superficially valid Z39.2
	// record: the length is encoded in the first five bytes and
	// the final byte is a recordTerminator.
	if !validLeader(rawData) {
		return nil, errInvalidLeader
	}

	m := new(MarcRecord)

	m.RawRecord = rawData
	m.Status = rawData[5]
	m.Type = rawData[6]
	m.BibLevel = rawData[7]
	m.CharacterEncoding = rawData[8]
	m.EncodingLevel = rawData[17]
	m.CatalogingForm = rawData[18]
	m.MultipartLevel = rawData[19]

	m.Directory = decodeDirectory(rawData)

	return m, nil
}

func (m *MarcRecord) GetRawField(tag string) VariableField {
	entry := m.Directory[tag]
	if entry == nil {
		return VariableField{}
	}

	result := make([][]byte, len(entry))
	for i := range entry {
		start := entry[i].startingOffset;
		end := entry[i].startingOffset + entry[i].fieldLength
		result[i] = m.RawRecord[start:end]
	}

	return VariableField{result}
}

func (f *VariableField) ValueCount() int {
	return len(f.rawData)
}

func (f *VariableField) GetRawValue(i int) []byte {
	return f.rawData[i]
}

func validLeader(leader []byte) bool {
	for i := range marc21LeaderValues {
		s := string(leader[marc21LeaderValues[i].offset])
		if strings.IndexAny(marc21LeaderValues[i].values, s) == -1 {
			return false
		}
	}
	return true
}

func decodeDirectory(record []byte) map[string][]Entry {
	baseAddress := decodeDecimal(record[12:17])

	m := make(map[string][]Entry)

	for i := 24; record[i] != fieldTerminator; i += 12 {
		tag := string(record[i : i+3])
		m[tag] = append(m[tag],
			Entry{decodeDecimal(record[i+3 : i+7]), baseAddress + decodeDecimal(record[i+7:i+12])})
	}

	return m
}

func readRecord(r io.Reader) (int, []byte, error) {
	tmp := make([]byte, 5)

	_, e := r.Read(tmp)
	if e != nil {
		return 0, nil, e
	}

	rlen := decodeDecimal(tmp)
	if rlen < leaderSize+2 || rlen > maxRecordSize {
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

	if result[len(result)-1] != recordTerminator {
		return 0, nil, errNoRecordTerminator
	}

	return rlen, result, nil
}

func decodeDecimal(n []byte) int {
	result := 0
	for i := range n {
		result = (10 * result) + int(n[i]-'0')
	}
	return result
}
