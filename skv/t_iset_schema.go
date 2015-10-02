// Copyright 2015 lessOS.com, All rights reserved.
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
	IsetTypeBinary    uint8 = 0
	IsetTypeUint      uint8 = 1
	IsetTypeString    uint8 = 2
	IsetTypeHexString uint8 = 3

	//
	IsetKeyLenMax             = 16
	IsetPriLenMax             = 16
	IsetSchemaMaxIndex        = 20
	IsetScanMax        uint64 = 10000
	_iset_indexing            = false
)

type IsetSchema struct {
	Version uint32      `json:"version"`
	Indexes []IsetEntry `json:"indexes"`
}

func (is *IsetSchema) IndexEntry(column string) *IsetEntry {

	column = IsetIndexStringFilter(column)

	for _, ie := range is.Indexes {

		if ie.Column == column {
			return &ie
		}
	}

	return nil
}

type IsetEntry struct {
	Column   string `json:"column"`
	Type     uint8  `json:"type"`
	Length   uint8  `json:"length,omitempty"`
	Unique   bool   `json:"unique,omitempty"`
	AutoIncr bool   `json:"autoincr,omitempty"`
	Seq      uint8  `json:"seq,omitempty"`
}

type IsetEntryBytes struct {
	Seq       uint8
	Unique    bool
	AutoIncr  bool
	FieldName string
	Data      []byte
}

func NewIschema(version uint32) IsetSchema {
	return IsetSchema{
		Version: version,
	}
}

func (is *IsetSchema) IndexEntryAdd(ie IsetEntry) {

	if len(is.Indexes) > IsetSchemaMaxIndex {
		return
	}

	ie.Column = IsetIndexStringFilter(ie.Column)

	for _, prev := range is.Indexes {

		if prev.Column == ie.Column {
			return
		}
	}

	is.Indexes = append(is.Indexes, ie)
}

func IsetIndexDataExport(indexes map[string]IsetSchema, key []byte, obj map[string]interface{}) map[uint8]IsetEntryBytes {

	ls := map[uint8]IsetEntryBytes{}

	schema, ok := indexes[string(key)]
	if !ok {
		return ls
	}

	for mk, mv := range obj {

		if ie := schema.IndexEntry(mk); ie != nil {

			if bs, ok := IsetIndexValue(ie, reflect.ValueOf(mv)); ok {

				ls[ie.Seq] = IsetEntryBytes{
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

				ls[ie.Seq] = IsetEntryBytes{
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
