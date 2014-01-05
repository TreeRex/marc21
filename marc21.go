// Copyright 2013 Thomas Emerson. All rights reserved.

// Package marc21 implements a MARC-21 reader.
package marc21

import (
	"errors"
	"fmt"
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

// A transcoder transcodes a slice into a Unicode string.
type transcoderFunc func(bytes []byte) (string, error)

// FIXME: location is not a good name for this
type location struct {
	offset int
	length int
}

type VariableField struct {
	tag        string
	rawData    [][]byte
	transcoder transcoderFunc
}

// The identifier length and indicator count are not stored because they are
// constant in MARC 21 (2 octets each).
type MarcRecord struct {
	RawRecord         []byte
	Status            byte
	Type              byte
	BibLevel          byte
	CharacterEncoding byte
	EncodingLevel     byte
	CatalogingForm    byte
	MultipartLevel    byte
	Directory         map[string][]location
	transcoder        transcoderFunc
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
	{10, "2"}, // constant integer
	{11, "2"}, // constant integer
	// 12 -- 16 Base address of data
	{17, " 1234578uz"},
	{18, " aciu"},
	{19, " abc"},
	{20, "4"}, // constant integer
	{21, "5"}, // constant integer
	{22, "0"}, // constant integer
	{23, "0"}, // constant integer
}

//
// MarcRecord Functions
//

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

	switch m.CharacterEncoding {
	case ' ':
		m.transcoder = marc8Transcoder
	case 'a':
		m.transcoder = utf8Transcoder
	default:
		return nil, fmt.Errorf("Unknown Character Encoding \"%s\"", string(m.CharacterEncoding))
	}

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
		start := entry[i].offset
		end := entry[i].offset + entry[i].length
		result[i] = m.RawRecord[start:end]
	}

	return VariableField{tag, result, m.transcoder}
}

func (m *MarcRecord) GetControlField(tag string) (string, error) {
	if !isControlFieldTag(tag) {
		return "", fmt.Errorf("marc21: \"%s\" is not a valid control field", tag)
	}

	field := m.GetRawField(tag)
	if field.ValueCount() == 0 {
		// a missing field is not considered an error
		return "", nil
	}
	if field.ValueCount() > 1 {
		return "", fmt.Errorf("marc21: too many instances of control field \"%s\"", tag)
	}
	return string(field.rawData[0]), nil
}

func (m *MarcRecord) GetDataField(tag string) (VariableField, error) {
	if isControlFieldTag(tag) {
		return VariableField{}, fmt.Errorf("marc21: \"%s\" is not a data field", tag)
	}
	field := m.GetRawField(tag)
	return field, nil
}

//
// Variable Field functions
//

func (f *VariableField) ValueCount() int {
	return len(f.rawData)
}

func (f *VariableField) GetRawValue(i int) []byte {
	return f.rawData[i]
}

func (f *VariableField) IsControlField() bool {
	return isControlFieldTag(f.tag)
}

const (
	startSubfield = iota
	recordSubfield
	skipSubfield
)

func (f *VariableField) GetNthRawSubfield(subfield string, index int) []byte {
	instance := f.GetRawValue(index)

	state := startSubfield
	offset, start := 2, 0

loop:
	for {
		switch {
		case state == startSubfield:
			if instance[offset] == delimiter {
				if subfield[0] == instance[offset+1] {
					// found the subfield of interest
					start = offset + 2
					state = recordSubfield
				} else {
					state = skipSubfield
				}
				offset += 1
			} else {
				// if we get here we've either hit the fieldTerminator
				// of the record is corrupt
				break loop
			}
		case state == recordSubfield:
			if instance[offset] == delimiter || instance[offset] == fieldTerminator {
				return instance[start:offset]
			}
		case state == skipSubfield:
			if instance[offset] == delimiter {
				state = startSubfield
				// "push back" the delimiter
				offset -= 1
			} else if instance[offset] == fieldTerminator {
				break loop
			}
		}
		offset += 1
	}
	return nil
}

func (f *VariableField) GetNthSubfield(subfield string, index int) string {
	raw := f.GetNthRawSubfield(subfield, index)
	if raw != nil {
		t, _ := f.transcoder(raw)
		return t
	}
	return ""
}

func utf8Transcoder(bytes []byte) (string, error) {
	return string(bytes), nil
}

func marc8Transcoder(bytes []byte) (string, error) {
	// FIXME: write the marc8Transcoder
	return string(bytes), nil
}

//
// Internal functions
//

func validLeader(leader []byte) bool {
	for i := range marc21LeaderValues {
		s := string(leader[marc21LeaderValues[i].offset])
		if strings.IndexAny(marc21LeaderValues[i].values, s) == -1 {
			return false
		}
	}
	return true
}

func isControlFieldTag(tag string) bool {
	return tag[0] == '0' && tag[1] == '0'
}

func decodeDirectory(record []byte) map[string][]location {
	baseAddress := decodeDecimal(record[12:17])

	m := make(map[string][]location)

	for i := 24; record[i] != fieldTerminator; i += 12 {
		tag := string(record[i : i+3])
		m[tag] = append(m[tag],
			location{baseAddress + decodeDecimal(record[i+7:i+12]), decodeDecimal(record[i+3 : i+7])})
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
