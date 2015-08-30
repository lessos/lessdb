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

package leveldb

import (
	"reflect"

	"github.com/jmhodges/levigo"
	"github.com/lessos/lessdb/skv"
)

const (
	IsetTypeBinary    uint8 = 0
	IsetTypeUint      uint8 = 1
	IsetTypeString    uint8 = 2
	IsetTypeHexString uint8 = 3
)

const (
	_iset_keylen_max              = 16
	_iset_prilen_max              = 16
	_iset_schema_max_index        = 20
	_iset_indexing                = false
	_iset_scan_max         uint64 = 10000
)

type IsetSchema struct {
	Version uint32      `json:"version"`
	Indexes []IsetEntry `json:"indexes"`
}

func (is *IsetSchema) IndexEntry(column string) *IsetEntry {

	column = _iset_idx_string_filter(column)

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

	if len(is.Indexes) > _iset_schema_max_index {
		return
	}

	ie.Column = _iset_idx_string_filter(ie.Column)

	for _, prev := range is.Indexes {

		if prev.Column == ie.Column {
			return
		}
	}

	is.Indexes = append(is.Indexes, ie)
}

func (db *DB) IschemaSet(key []byte, schema IsetSchema) *skv.Reply {

	_iset_global_locker.Lock()
	defer _iset_global_locker.Unlock()

	var (
		rpl  = skv.NewReply("")
		prev IsetSchema
	)

	if len(schema.Indexes) > _iset_schema_max_index {
		rpl.Status = skv.ReplyInvalidArgument
		return rpl
	}

	if rs := db._raw_get(_iset_schema_key(key)); rs.Status == "OK" {
		rs.JsonDecode(&prev)
	}

	for i, ei := range schema.Indexes {
		ei.Column = _iset_idx_string_filter(ei.Column)
		schema.Indexes[i] = ei
	}

	for _, pi := range prev.Indexes {

		removed, changed := true, false

		for j, ei := range schema.Indexes {

			if pi.Column == ei.Column {

				removed = false

				if pi.Type != ei.Type ||
					pi.Length != ei.Length ||
					pi.Unique != ei.Unique ||
					pi.AutoIncr != ei.AutoIncr {

					changed = true
				}

				ei.Seq = pi.Seq

				schema.Indexes[j] = ei

				break
			}
		}

		if removed || changed {

			objIdxKeyPrefix, limit := _iset_idx_field_prefix(key, pi.Seq), 1000

			// fmt.Println("WARN CLEAN prev INDEXES", pi.Column)
			for {

				rs := db._raw_scan(objIdxKeyPrefix, objIdxKeyPrefix, uint64(limit)).Hash()

				if len(rs) > 0 {

					wb := levigo.NewWriteBatch()
					wo := levigo.NewWriteOptions()

					for _, entry := range rs {
						wb.Delete(entry.Key)
					}

					db.ldb.Write(wo, wb)

					wb.Close()
					wo.Close()
				}

				if len(rs) < limit {
					break
				}
			}
		}
	}

	for i, ei := range schema.Indexes {

		reidx := true

		for _, pi := range prev.Indexes {

			if pi.Column == ei.Column {

				ei.Seq = pi.Seq

				schema.Indexes[i] = ei

				if pi.Type == ei.Type &&
					pi.Length == ei.Length &&
					pi.Unique == ei.Unique &&
					pi.AutoIncr == ei.AutoIncr &&
					pi.Seq > 0 {

					reidx = false
				}

				break
			}
		}

		//
		for _, ci := range schema.Indexes {

			if ei.Seq == ci.Seq && ei.Column != ci.Column {
				ei.Seq = 0
				schema.Indexes[i] = ei
				break
			}
		}

		if ei.Seq == 0 {

			reidx = true

			for j := uint8(1); j <= 255; j++ {

				dup := false

				for _, ci := range schema.Indexes {

					if j == ci.Seq && ei.Column != ci.Column {
						dup = true
						break
					}
				}

				if !dup {
					ei.Seq = j
					schema.Indexes[i] = ei
					break
				}
			}
		}

		if reidx && ei.Seq > 0 {

			// fmt.Println("WARN NEW INDEXES", ei.Column, ei.Seq)

			offset, limit := []byte{}, 1000

			for {

				rs := db.Iscan(key, offset, []byte{}, uint64(limit)).Hash()

				wb := levigo.NewWriteBatch()
				wo := levigo.NewWriteOptions()

				for _, entry := range rs {

					var obj map[string]interface{}
					if err := entry.JsonDecode(&obj); err == nil {

						for mk, mv := range obj {

							mk = _iset_idx_string_filter(mk)

							if mk != ei.Column {
								continue
							}

							if bs, ok := _iset_idx_value(&ei, reflect.ValueOf(mv)); ok {
								wb.Put(append(append(_iset_idx_field_prefix(key, ei.Seq), bs...), entry.Key...), []byte{})
							}

							break
						}

						offset = entry.Key
					}
				}

				db.ldb.Write(wo, wb)
				wb.Close()
				wo.Close()

				if len(rs) < limit {
					break
				}
			}
		}
	}

	skey := string(key)

	rpl = db._raw_set_json(_iset_schema_key(key), schema, 0)
	if rpl.Status == "OK" {
		_iset_indexes[skey] = schema
	}

	return rpl
}
