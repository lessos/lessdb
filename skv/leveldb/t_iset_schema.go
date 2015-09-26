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

func (db *DB) IschemaSet(key []byte, schema skv.IsetSchema) *skv.Reply {

	_iset_global_locker.Lock()
	defer _iset_global_locker.Unlock()

	var (
		rpl  = skv.NewReply("")
		prev skv.IsetSchema
	)

	if len(schema.Indexes) > skv.IsetSchemaMaxIndex {
		rpl.Status = skv.ReplyInvalidArgument
		return rpl
	}

	if rs := db._raw_get(skv.IsetSchemaKey(key)); rs.Status == "OK" {
		rs.JsonDecode(&prev)
	}

	for i, ei := range schema.Indexes {
		ei.Column = skv.IsetIndexStringFilter(ei.Column)
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

			objIdxKeyPrefix, limit := skv.IsetIndexFieldPrefix(key, pi.Seq), 1000

			// fmt.Println("WARN CLEAN prev INDEXES", pi.Column)
			for {

				rs := db._raw_scan(objIdxKeyPrefix, objIdxKeyPrefix, uint64(limit)).Hash()

				if len(rs) > 0 {

					wb := levigo.NewWriteBatch()
					// wo := levigo.NewWriteOptions()

					for _, entry := range rs {
						wb.Delete(entry.Key)
					}

					db.ldb.Write(db.writeOpts, wb)

					wb.Close()
					// wo.Close()
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
				// wo := levigo.NewWriteOptions()

				for _, entry := range rs {

					var obj map[string]interface{}
					if err := entry.JsonDecode(&obj); err == nil {

						for mk, mv := range obj {

							mk = skv.IsetIndexStringFilter(mk)

							if mk != ei.Column {
								continue
							}

							if bs, ok := skv.IsetIndexValue(&ei, reflect.ValueOf(mv)); ok {
								wb.Put(append(append(skv.IsetIndexFieldPrefix(key, ei.Seq), bs...), entry.Key...), []byte{})
							}

							break
						}

						offset = entry.Key
					}
				}

				db.ldb.Write(db.writeOpts, wb)
				wb.Close()
				// wo.Close()

				if len(rs) < limit {
					break
				}
			}
		}
	}

	skey := string(key)

	rpl = db._raw_set_json(skv.IsetSchemaKey(key), schema, 0)
	if rpl.Status == "OK" {
		_iset_indexes[skey] = schema
	}

	return rpl
}
