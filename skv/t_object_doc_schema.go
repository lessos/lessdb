// Copyright 2015-2016 lessdb Author, All rights reserved.
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

package skv

import (
	"reflect"
)

const (
	//
	ObjectDocSchemaIndexTypeBinary    uint8 = 0
	ObjectDocSchemaIndexTypeUint      uint8 = 1
	ObjectDocSchemaIndexTypeString    uint8 = 2
	ObjectDocSchemaIndexTypeHexString uint8 = 3

	//
	ObjectDocKeyLenMax             = 16
	ObjectDocPriLenMax             = 16
	ObjectDocSchemaMaxIndex        = 20
	ObjectDocScanMax        uint32 = 10000
)

type ObjectDocSchema struct {
	Version uint32                      `json:"version"`
	Indexes []ObjectDocSchemaIndexEntry `json:"indexes"`
}

func (is *ObjectDocSchema) IndexEntry(column string) *ObjectDocSchemaIndexEntry {

	column = ObjectDocIndexStringFilter(column)

	for _, ie := range is.Indexes {

		if ie.Column == column {
			return &ie
		}
	}

	return nil
}

type ObjectDocSchemaIndexEntry struct {
	Column   string `json:"column"`
	Type     uint8  `json:"type"`
	Length   uint8  `json:"length,omitempty"`
	Unique   bool   `json:"unique,omitempty"`
	AutoIncr bool   `json:"autoincr,omitempty"`
	Seq      uint8  `json:"seq,omitempty"`
}

type ObjectDocSchemaIndexEntryBytes struct {
	Seq       uint8
	Unique    bool
	AutoIncr  bool
	FieldName string
	Data      []byte
}

func NewObjectDocSchema(version uint32) ObjectDocSchema {
	return ObjectDocSchema{
		Version: version,
	}
}

func (is *ObjectDocSchema) IndexEntryAdd(ie ObjectDocSchemaIndexEntry) {

	if len(is.Indexes) > ObjectDocSchemaMaxIndex {
		return
	}

	ie.Column = ObjectDocIndexStringFilter(ie.Column)

	for _, prev := range is.Indexes {

		if prev.Column == ie.Column {
			return
		}
	}

	is.Indexes = append(is.Indexes, ie)
}

func ObjectDocIndexDataExport(indexes map[string]ObjectDocSchema, key []byte, obj map[string]interface{}) map[uint8]ObjectDocSchemaIndexEntryBytes {

	ls := map[uint8]ObjectDocSchemaIndexEntryBytes{}

	schema, ok := indexes[string(key)]
	if !ok {
		return ls
	}

	for mk, mv := range obj {

		if ie := schema.IndexEntry(mk); ie != nil {

			if bs, ok := ObjectDocIndexValue(ie, reflect.ValueOf(mv)); ok {

				ls[ie.Seq] = ObjectDocSchemaIndexEntryBytes{
					Unique:    ie.Unique,
					AutoIncr:  ie.AutoIncr,
					Data:      bs,
					FieldName: mk,
				}
			}

			if len(ls) >= len(schema.Indexes) {
				break
			}
		}
	}

	for _, ie := range schema.Indexes {

		if ie.AutoIncr {

			if _, ok := ls[ie.Seq]; !ok {

				ls[ie.Seq] = ObjectDocSchemaIndexEntryBytes{
					Unique:    ie.Unique,
					AutoIncr:  ie.AutoIncr,
					Data:      make([]byte, ie.Length),
					FieldName: ie.Column,
				}
			}
		}
	}

	return ls
}
