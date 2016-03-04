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

package goleveldb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"strconv"
	"sync"

	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb"
)

type empty struct{}

var (
	_obj_doc_global_locker sync.Mutex
	_obj_doc_indexes       = map[string]skv.ObjectDocSchema{}
)

func (db *DB) ObjectDocSchemaSync(fold string, schema skv.ObjectDocSchema) *skv.Reply {

	_obj_doc_global_locker.Lock()
	defer _obj_doc_global_locker.Unlock()

	var (
		rpl  = skv.NewReply("")
		key  = skv.ObjectDocFoldKey(fold)
		prev skv.ObjectDocSchema
	)

	if len(schema.Indexes) > skv.ObjectDocSchemaMaxIndex {
		rpl.Status = skv.ReplyBadArgument
		return rpl
	}

	if rs := db._raw_get(skv.ObjectDocSchemaKey(key)); rs.Status == skv.ReplyOK {
		rs.JsonDecode(&prev)
	}

	for i, ei := range schema.Indexes {
		ei.Column = skv.ObjectDocIndexStringFilter(ei.Column)
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

			objIdxKeyPrefix, limit := skv.ObjectDocIndexFieldPrefix(key, pi.Seq), 1000

			// fmt.Println("WARN CLEAN prev INDEXES", pi.Column)
			for {

				rs := db._raw_scan(objIdxKeyPrefix, objIdxKeyPrefix, uint32(limit)).Hash()

				if len(rs) > 0 {
					batch := new(leveldb.Batch)

					for _, entry := range rs {
						batch.Delete(entry.Key)
					}

					db.ldb.Write(batch, nil)
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

			offset, limit := "", 1000

			for {

				rs := db.ObjectScan(fold, offset, "", uint32(limit)).ObjectList()

				batch := new(leveldb.Batch)

				for _, entry := range rs {

					var obj map[string]interface{}
					if err := entry.JsonDecode(&obj); err == nil {

						for mk, mv := range obj {

							mk = skv.ObjectDocIndexStringFilter(mk)

							if mk != ei.Column {
								continue
							}

							if bs, ok := skv.ObjectDocIndexValue(&ei, reflect.ValueOf(mv)); ok {
								batch.Put(skv.BytesConcat(skv.ObjectDocIndexFieldPrefix(key, ei.Seq), bs, entry.Key), []byte{})
							}

							break
						}

						offset = skv.BytesToHexString(entry.Key)
					}
				}

				db.ldb.Write(batch, nil)

				if len(rs) < limit {
					break
				}
			}
		}
	}

	skey := string(key)

	rpl = db._raw_put_json(skv.ObjectDocSchemaKey(key), schema, 0)
	if rpl.Status == skv.ReplyOK {
		_obj_doc_indexes[skey] = schema
	}

	return rpl
}

func (db *DB) ObjectDocGet(fold, key string) *skv.Reply {
	return db._raw_get(skv.NewObjectPathKey(fold, key).EntryIndex())
}

func (db *DB) ObjectDocPut(fold, key string, obj interface{}, opts *skv.ObjectWriteOptions) *skv.Reply {

	_obj_doc_global_locker.Lock()
	_obj_doc_global_locker.Unlock()

	var (
		opath = skv.NewObjectPathKey(fold, key)
		rpl   = skv.NewReply("")
	)

	if len(opath.Fold) > skv.ObjectDocKeyLenMax ||
		len(opath.Field) > skv.ObjectDocPriLenMax ||
		obj == nil {
		rpl.Status = skv.ReplyBadArgument
		return rpl
	}

	var (
		bkey    = opath.EntryIndex()
		objt    = reflect.TypeOf(obj)
		objv    = reflect.ValueOf(obj)
		prev    = map[string]interface{}{}
		previdx = map[uint8]skv.ObjectDocSchemaIndexEntryBytes{}
		set     = map[string]interface{}{}
	)

	if opts == nil {
		opts = _obj_options_def
	}

	prevobj := db._raw_get(bkey).Object()
	if prevobj.Status == skv.ReplyOK {

		if err := prevobj.JsonDecode(&prev); err == nil {
			previdx = skv.ObjectDocIndexDataExport(_obj_doc_indexes, opath.Fold, prev)
		}
	}

	if objt.Kind() == reflect.Struct {

		for i := 0; i < objt.NumField(); i++ {
			set[skv.ObjectDocIndexStringFilter(objt.Field(i).Name)] = objv.Field(i).Interface()
		}

	} else if objt.Kind() == reflect.Map {

		mks := objv.MapKeys()

		for _, mkv := range mks {

			if mkv.Kind() == reflect.String {
				set[skv.ObjectDocIndexStringFilter(mkv.String())] = objv.MapIndex(mkv).Interface()
			}
		}

	} else {

		rpl.Status = skv.ReplyBadArgument
		return rpl
	}

	setidx, idxnew, idxdup := skv.ObjectDocIndexDataExport(_obj_doc_indexes, opath.Fold, set), [][]byte{}, [][]byte{}

	// fmt.Println("\tsetidx", setidx)
	// fmt.Println("\tprevidx", previdx)

	for siKey, siEntry := range setidx {

		var incr_set, incr_prev uint64

		if siEntry.AutoIncr {
			incr_set = skv.BytesToUint64(siEntry.Data)
		}

		//
		if piEntry, ok := previdx[siKey]; ok {

			if siEntry.AutoIncr && incr_set == 0 {

				if incr_prev = skv.BytesToUint64(piEntry.Data); incr_prev > 0 {

					siEntry.Data, incr_set = piEntry.Data, incr_prev

					set[siEntry.FieldName] = incr_set

					continue
				}

			} else if bytes.Compare(piEntry.Data, siEntry.Data) == 0 {
				continue
			}

			idxdup = append(idxdup, append(append(skv.ObjectDocIndexFieldPrefix(opath.Fold, siKey), piEntry.Data...), opath.Field...))
		}

		//
		if siEntry.AutoIncr {

			if incr_set == 0 {

				incr_set = db._raw_incrby(skv.ObjectDocIndexIncrKey(opath.Fold, siEntry.Seq), 1).Uint64()

				ibs := make([]byte, 8)
				binary.BigEndian.PutUint64(ibs, incr_set)

				siEntry.Data = ibs[(8 - len(siEntry.Data)):]

				set[siEntry.FieldName] = incr_set

			} else if incr_set > 0 && incr_set > incr_prev {

				if db._raw_get(skv.ObjectDocIndexIncrKey(opath.Fold, siEntry.Seq)).Uint64() < incr_set {
					db._raw_put(skv.ObjectDocIndexIncrKey(opath.Fold, siEntry.Seq), []byte(strconv.FormatUint(incr_set, 10)), 0)
				}
			}
		}

		if siEntry.Unique || siEntry.AutoIncr {

			objIdxKeyPrefix := append(skv.ObjectDocIndexFieldPrefix(opath.Fold, siKey), siEntry.Data...)

			if rs := db._raw_scan(objIdxKeyPrefix, []byte{}, 1).Hash(); len(rs) > 0 {
				rpl.Status = skv.ReplyBadArgument
				return rpl
			}
		}

		idxnew = append(idxnew, append(append(skv.ObjectDocIndexFieldPrefix(opath.Fold, siKey), siEntry.Data...), opath.Field...))
	}

	//

	//
	batch := new(leveldb.Batch)

	for _, idxkey := range idxdup {
		batch.Delete(idxkey)
	}

	for _, idxkey := range idxnew {
		batch.Put(idxkey, []byte{})
	}

	bvalue, _ := skv.JsonEncode(set)
	sum := crc32.ChecksumIEEE(bvalue)

	if prevobj.Meta.Sum == sum {
		return skv.NewReply(skv.ReplyOK)
	}

	db._obj_meta_sync(skv.ObjectTypeDocument, &prevobj.Meta, opath, int64(len(bvalue)), sum, _obj_options_def)

	batch.Put(bkey, append(prevobj.Meta.Export(), bvalue...))

	if err := db.ldb.Write(batch, nil); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) ObjectDocDel(fold, key string) *skv.Reply {

	var (
		rpl     = skv.NewReply("")
		opath   = skv.NewObjectPathKey(fold, key)
		bkey    = opath.EntryIndex()
		prevobj *skv.Object
		previdx = map[uint8]skv.ObjectDocSchemaIndexEntryBytes{}
	)

	if rs := db._raw_get(bkey); rs.Status == skv.ReplyNotFound {

		return rpl

	} else if rs.Status != skv.ReplyOK {

		return rs

	} else {

		prevobj = rs.Object()

		var prev map[string]interface{}

		if err := prevobj.JsonDecode(&prev); err == nil {
			previdx = skv.ObjectDocIndexDataExport(_obj_doc_indexes, opath.Fold, prev)
		}
	}

	batch := new(leveldb.Batch)

	for piKey, piEntry := range previdx {
		batch.Delete(append(append(skv.ObjectDocIndexFieldPrefix(opath.Fold, piKey), piEntry.Data...), opath.Field...))
	}

	batch.Delete(bkey)

	if err := db.ldb.Write(batch, nil); err != nil {
		rpl.Status = err.Error()
	} else {
		db._obj_meta_sync(prevobj.Meta.Type, &prevobj.Meta, opath, -1, 0, _obj_options_def)

		// if _obj_event_handler != nil {
		//     _obj_event_handler(opath, skv.ObjectEventDeleted, 0)
		// }
	}

	return rpl
}

// TODO btree
//  https://github.com/petar/GoLLRB
//  https://github.com/google/btree
func (db *DB) ObjectDocQuery(fold string, qry *skv.ObjectDocQuerySet) *skv.Reply {

	var (
		rpl  = skv.NewReply(skv.ReplyBadArgument)
		key  = skv.ObjectDocFoldKey(fold)
		skey = string(key)
	)

	schema, ok := _obj_doc_indexes[skey]
	if !ok {
		return rpl
	}

	idxs := map[string]skv.ObjectDocSchemaIndexEntry{}
	for _, idx := range schema.Indexes {

		if qry.SortField == idx.Column && idx.Type != skv.ObjectDocSchemaIndexTypeUint {
			return rpl
		}

		idxs[idx.Column] = idx
	}

	for _, filter := range qry.Filters {
		if _, ok := idxs[filter.Field]; !ok {
			return rpl
		}
	}

	sls := [][]byte{}

	if idx, ok := idxs[qry.SortField]; ok {

		start, end := skv.ObjectDocIndexFieldPrefix(key, idx.Seq), skv.ObjectDocIndexFieldPrefix(key, idx.Seq)

		rs := []skv.Entry{}

		for {

			if qry.SortMode == skv.ObjectDocQuerySortAttrDesc {
				rs = db._raw_revscan(start, end, skv.ObjectDocScanMax).Hash()
			} else {
				rs = db._raw_scan(start, end, skv.ObjectDocScanMax).Hash()
			}

			for _, v := range rs {

				if _, bkey, ok := skv.ObjectDocIndexRawKeyExport(v.Key, idx.Length); ok {
					sls = append(sls, skv.BytesClone(bkey))
				}

				if qry.SortMode == skv.ObjectDocQuerySortAttrDesc {
					end = skv.ObjectDocBytesDecr(v.Key)
				} else {
					start = skv.ObjectDocBytesIncr(v.Key)
				}
			}

			if uint32(len(rs)) < skv.ObjectDocScanMax {
				break
			}
		}
	}

	sls_ok := false
	if len(sls) > 0 {
		sls_ok = true
	}

	for _, filter := range qry.Filters {

		idx, ok := idxs[filter.Field]
		if !ok {
			continue
		}

		if idx.Type != skv.ObjectDocSchemaIndexTypeUint {
			continue
		}

		vstart, vend, values := []byte{}, []byte{}, [][]byte{}

		for _, v := range filter.Values {

			vb := skv.SintToBytes(v, idx.Length)

			dup := false
			for _, pvb := range values {

				if bytes.Compare(pvb, vb) == 0 {
					dup = true
					break
				}
			}

			if !dup {

				values = append(values, vb)

				if (filter.Type == skv.ObjectDocQueryFilterValues && !filter.Exclude) ||
					filter.Type == skv.ObjectDocQueryFilterRange {

					if len(vstart) < 1 {
						vstart = vb
					} else if bytes.Compare(vb, vstart) < 1 {
						vstart = vb
					}

					if bytes.Compare(vb, vend) > 0 {
						vend = vb
					}
				}
			}
		}

		var (
			kpre    = skv.ObjectDocIndexFieldPrefix(key, idx.Seq)
			start   = append(kpre, vstart...)
			end     = append(kpre, vend...)
			fitkeys = map[string]empty{}
		)

		for {

			rs := db._raw_scan(start, end, skv.ObjectDocScanMax).Hash()

			for _, v := range rs {

				if _, bkey, ok := skv.ObjectDocIndexRawKeyExport(v.Key, idx.Length); ok {

					if sls_ok {

						fitkeys[string(bkey)] = empty{}

					} else {
						sls = append(sls, skv.BytesClone(bkey))
					}
				}

				start = skv.ObjectDocBytesIncr(v.Key)
			}

			if uint32(len(rs)) < skv.ObjectDocScanMax {
				break
			}
		}

		if sls_ok {

			sls_buf := sls
			sls = [][]byte{}

			for _, gv := range sls_buf {

				if _, ok := fitkeys[string(gv)]; ok {
					sls = append(sls, gv)
				}
			}
		}

		sls_ok = true
	}

	if !sls_ok {

		// TOPO
		tls := db.ObjectScan(fold, "", "", uint32(qry.Offset+qry.Limit)).Hash()
		for i := qry.Offset; i < len(tls); i++ {
			rpl.Data = append(rpl.Data, tls[i].Key, tls[i].Value)
		}

		return rpl
	}

	if len(sls) <= qry.Offset {
		return rpl
	}

	cutoff := qry.Offset + qry.Limit
	if cutoff > len(sls) {
		cutoff = len(sls)
	}

	for i := qry.Offset; i < cutoff; i++ {
		if rs := db.ObjectDocGet(fold, skv.BytesToHexString(sls[i])); rs.Status == skv.ReplyOK {
			rpl.Data = append(rpl.Data, sls[i], rs.Bytes())
		}
	}

	rpl.Status = skv.ReplyOK

	return rpl
}
