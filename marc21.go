// Copyright 2013-14 Thomas Emerson
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package marc21 implements a MARC-21 reader.
package marc21

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
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
	Tag        string
	rawData    [][]byte
	transcoder transcoderFunc
}

type Reader struct {
	r io.Reader
	validate bool
	offset uint64
}

type MarcRecord struct {
	RawRecord         []byte
	Offset            uint64
	Status            byte
	Type              byte
	BibLevel          byte
	CharacterEncoding byte
	EncodingLevel     byte
	CatalogingForm    byte
	MultipartLevel    byte
	Directory         map[string][]location
	transcoder        transcoderFunc
	// The identifier length and indicator count are not stored because they
	// are constant in MARC 21 (2 octets each).
}

// marc21LeaderValues provies the valid values for positions in the MARC 21
// Bibliographic leader per http://www.loc.gov/marc/bibliographic/bdleader.html .
// Unfortunately in the real world it is common for records to have values that
// are not allowed by the spec.
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

func NewReader(rdr io.Reader, validate bool) *Reader {
	nr := new(Reader)
	nr.r = rdr
	nr.validate = validate
	nr.offset = 0
	return nr
}

func (r *Reader) Next() (*MarcRecord, error) {
	rlen, raw, err := readRecord(r.r)
	if (err == io.EOF) {
		return nil, nil
	} else if (err != nil) {
		return nil, err
	}
	offset := r.offset
	r.offset += uint64(rlen)

	return NewMarcRecord(raw, r.validate, offset)
}

//
// MarcRecord Functions
//

func NewMarcRecord(rawData []byte, validate bool, offset uint64) (*MarcRecord, error) {
	// this assumes that rawData is a superficially valid Z39.2
	// record: the length is encoded in the first five bytes and
	// the final byte is a recordTerminator.
	if validate && !validLeader(rawData) {
		return nil, errInvalidLeader
	}

	m := new(MarcRecord)

	m.RawRecord = rawData
	m.Offset = offset
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
		// I'm not sure what the best thing to do is here: if leader validation
		// is turned off we will get here, which is problematic
		m.transcoder = utf8Transcoder
		//return nil, fmt.Errorf("Unknown Character Encoding \"%s\"", string(m.CharacterEncoding))
	}

	m.Directory = decodeDirectory(rawData)

	return m, nil
}

// GetFieldList returns a sorted list of the field tags in the record.
func (m *MarcRecord) GetFieldList() []string {
	keys := make([]string, len(m.Directory))
	i := 0
	for k, _ := range m.Directory {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

// GetLeader returns the leader of the record
func (m *MarcRecord) GetLeader() string {
	return string(m.RawRecord[:leaderSize])
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
	if !IsControlFieldTag(tag) {
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

	cf := field.rawData[0]

	return string(cf[:len(cf) - 1]), nil
}

func (m *MarcRecord) GetDataField(tag string) (VariableField, error) {
	if IsControlFieldTag(tag) {
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
	return IsControlFieldTag(f.Tag)
}

const (
	startSubfield = iota
	recordSubfield
	skipSubfield
)

// GetSubfields returns a sorted list of subfield tags for the field
// instance specified by index.
func (f *VariableField) GetSubfields(index int) []string {
	instance := f.GetRawValue(index)

	subfields := make([]string, 0, 10)

	for i := range instance  {
		if instance[i] == delimiter {
			subfields = append(subfields, string(instance[i + 1]))
		}
	}
	sort.Strings(subfields)
	return subfields
}

func (f *VariableField) GetNthRawSubfield(subfield string, index int) []byte {
	rv := f.GetRawValue(index)
	i := 2
	sf := subfield[0]

	// in a properly formed record rv[i] will be a delimiter, but check anyway
	if rv[i] == delimiter {
delim:
		i++
		if rv[i] == sf {
			i++ ; start := i
			for rv[i] != delimiter && rv[i] != fieldTerminator {
				i++
			}
			return rv[start:i]
		}
		for {
			switch rv[i] {
			case delimiter:
				goto delim
			case fieldTerminator:
				return nil
			default:
				i++
			}
		}
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

func (f *VariableField) GetIndicators(index int) string {
	ind := ""
	if f.rawData[index][0] == ' ' {
		ind += "#"
	} else {
		ind += string(f.rawData[index][0])
	}
	if f.rawData[index][1] == ' ' {
		ind += "#"
	} else {
		ind += string(f.rawData[index][1])
	}
	return ind
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
			log.Printf("Leader position %d invalid, got %s expect one of '%s'\n",
				marc21LeaderValues[i].offset, s, marc21LeaderValues[i].values)
			return false
		}
	}
	return true
}

func IsControlFieldTag(tag string) bool {
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
