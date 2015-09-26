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
	"bytes"
	"encoding/binary"
	"reflect"
	"strconv"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/lessos/lessdb/skv"
)

var (
	_iset_global_locker sync.Mutex
	_iset_indexes       = map[string]skv.IsetSchema{}
)

type empty struct{}

func (db *DB) Iget(key, prikey []byte) *skv.Reply {
	return db._raw_get(skv.IsetEntryKey(key, prikey))
}

func (db *DB) Iscan(key, cursor, end []byte, limit uint64) *skv.Reply {

	if limit > skv.ScanMaxLimit {
		limit = skv.ScanMaxLimit
	}

	var (
		prefix = skv.IsetEntryKeyPrefix(key)
		prelen = len(prefix)
		cstart = append(prefix, cursor...)
		cend   = append(prefix, end...)
		rpl    = skv.NewReply("")
	)

	for i := len(cend); i < 256; i++ {
		cend = append(cend, 0xff)
	}

	// ro := levigo.NewReadOptions()
	// ro.SetFillCache(false)
	// defer ro.Close()

	// it := db.ldb.NewIterator(&util.Range{Start: cstart, Limit: append(cend)}, nil)
	it := db.ldb.NewIterator(db.iteratorReadOpts)
	defer it.Close()

	for it.Seek(cstart); it.Valid(); it.Next() {

		if limit < 1 {
			break
		}

		if len(it.Key()) < prelen {
			continue
		}

		if bytes.Compare(it.Key(), cend) > 0 {
			break
		}

		rpl.Data = append(rpl.Data, skv.BytesClone(it.Key()[prelen:]))
		rpl.Data = append(rpl.Data, skv.BytesClone(it.Value()))

		limit--
	}

	if err := it.GetError(); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

// TODO btree
// 	https://github.com/petar/GoLLRB
//  https://github.com/google/btree
func (db *DB) Iquery(key []byte, qry *skv.QuerySet) *skv.Reply {

	rpl := skv.NewReply(skv.ReplyInvalidArgument)
	skey := string(key)

	schema, ok := _iset_indexes[skey]
	if !ok {
		return rpl
	}

	idxs := map[string]skv.IsetEntry{}
	for _, idx := range schema.Indexes {

		if qry.SortField == idx.Column && idx.Type != skv.IsetTypeUint {
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

	if qry.SortField != "" {

		idx, ok := idxs[qry.SortField]
		if !ok {
			return rpl
		}

		start, end := skv.IsetIndexFieldPrefix(key, idx.Seq), skv.IsetIndexFieldPrefix(key, idx.Seq)

		rs := []skv.Entry{}

		for {

			if qry.SortMode == skv.QuerySortAttrDesc {
				rs = db._raw_revscan(start, end, skv.IsetScanMax).Hash()
			} else {
				rs = db._raw_scan(start, end, skv.IsetScanMax).Hash()
			}

			for _, v := range rs {

				if _, bkey, ok := skv.IsetIndexRawKeyExport(v.Key, idx.Length); ok {
					sls = append(sls, skv.BytesClone(bkey))
				}

				if qry.SortMode == skv.QuerySortAttrDesc {
					end = skv.IsetBytesDecr(v.Key)
				} else {
					start = skv.IsetBytesIncr(v.Key)
				}
			}

			if uint64(len(rs)) < skv.IsetScanMax {
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

		if idx.Type != skv.IsetTypeUint {
			continue
		}

		vstart, vend, values := []byte{}, []byte{}, [][]byte{}

		for _, v := range filter.Values {

			vb := skv.IsetIndexSintToBytes(v, idx.Length)

			dup := false
			for _, pvb := range values {

				if bytes.Compare(pvb, vb) == 0 {
					dup = true
					break
				}
			}

			if !dup {

				values = append(values, vb)

				if (filter.Type == skv.QueryFilterValues && !filter.Exclude) ||
					filter.Type == skv.QueryFilterRange {

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
			kpre    = skv.IsetIndexFieldPrefix(key, idx.Seq)
			start   = append(kpre, vstart...)
			end     = append(kpre, vend...)
			fitkeys = map[string]empty{}
		)

		for {

			rs := db._raw_scan(start, end, skv.IsetScanMax).Hash()

			for _, v := range rs {

				if _, bkey, ok := skv.IsetIndexRawKeyExport(v.Key, idx.Length); ok {

					if sls_ok {

						fitkeys[string(bkey)] = empty{}

					} else {
						sls = append(sls, skv.BytesClone(bkey))
					}
				}

				start = skv.IsetBytesIncr(v.Key)
			}

			if uint64(len(rs)) < skv.IsetScanMax {
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
		tls := db.Iscan(key, []byte{}, []byte{}, uint64(qry.Offset+qry.Limit)).Hash()
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
		if rs := db.Iget(key, sls[i]); rs.Status == "OK" {
			rpl.Data = append(rpl.Data, sls[i], rs.Bytes())
		}
	}

	rpl.Status = skv.ReplyOK

	return rpl
}

func (db *DB) Iset(key, prikey []byte, obj interface{}) *skv.Reply {

	_iset_global_locker.Lock()
	_iset_global_locker.Unlock()

	rpl := skv.NewReply("")

	if len(key) > skv.IsetKeyLenMax ||
		len(prikey) > skv.IsetPriLenMax ||
		obj == nil {
		rpl.Status = skv.ReplyInvalidArgument
		return rpl
	}

	var (
		bkey     = skv.IsetEntryKey(key, prikey)
		objt     = reflect.TypeOf(obj)
		objv     = reflect.ValueOf(obj)
		set      = map[string]interface{}{}
		prev     = map[string]interface{}{}
		previdx  = map[uint8]skv.IsetEntryBytes{}
		len_incr = false
	)

	if rs := db._raw_get(bkey); rs.Status == skv.ReplyOK {

		if err := rs.JsonDecode(&prev); err == nil {
			previdx = skv.IsetIndexDataExport(_iset_indexes, key, prev)
		}

	} else if rs.Status == skv.ReplyNotFound {
		len_incr = true
	}

	if objt.Kind() == reflect.Struct {

		for i := 0; i < objt.NumField(); i++ {
			set[skv.IsetIndexStringFilter(objt.Field(i).Name)] = objv.Field(i).Interface()
		}

	} else if objt.Kind() == reflect.Map {

		mks := objv.MapKeys()

		for _, mkv := range mks {

			if mkv.Kind() == reflect.String {
				set[skv.IsetIndexStringFilter(mkv.String())] = objv.MapIndex(mkv).Interface()
			}
		}

	} else {

		rpl.Status = skv.ReplyInvalidArgument
		return rpl
	}

	setidx, idxnew, idxdup := skv.IsetIndexDataExport(_iset_indexes, key, set), [][]byte{}, [][]byte{}

	// fmt.Println("\tsetidx", setidx)
	// fmt.Println("\tprevidx", previdx)

	for siKey, siEntry := range setidx {

		var incr_set, incr_prev uint64

		if siEntry.AutoIncr {
			incr_set = skv.IsetBytesToUint64(siEntry.Data)
		}

		//
		if piEntry, ok := previdx[siKey]; ok {

			if siEntry.AutoIncr && incr_set == 0 {

				if incr_prev = skv.IsetBytesToUint64(piEntry.Data); incr_prev > 0 {

					siEntry.Data, incr_set = piEntry.Data, incr_prev

					set[siEntry.FieldName] = incr_set

					continue
				}

			} else if bytes.Compare(piEntry.Data, siEntry.Data) == 0 {
				continue
			}

			idxdup = append(idxdup, append(append(skv.IsetIndexFieldPrefix(key, siKey), piEntry.Data...), prikey...))
		}

		//
		if siEntry.AutoIncr {

			if incr_set == 0 {

				incr_set = db._raw_incrby(skv.IsetIndexIncrKey(key, siEntry.Seq), 1).Uint64()

				ibs := make([]byte, 8)
				binary.BigEndian.PutUint64(ibs, incr_set)

				siEntry.Data = ibs[(8 - len(siEntry.Data)):]

				set[siEntry.FieldName] = incr_set

			} else if incr_set > 0 && incr_set > incr_prev {

				if db._raw_get(skv.IsetIndexIncrKey(key, siEntry.Seq)).Uint64() < incr_set {
					db._raw_set(skv.IsetIndexIncrKey(key, siEntry.Seq), []byte(strconv.FormatUint(incr_set, 10)), 0)
				}
			}
		}

		if siEntry.Unique || siEntry.AutoIncr {

			objIdxKeyPrefix := append(skv.IsetIndexFieldPrefix(key, siKey), siEntry.Data...)

			if rs := db._raw_scan(objIdxKeyPrefix, []byte{}, 1).Hash(); len(rs) > 0 {
				rpl.Status = skv.ReplyInvalidArgument
				return rpl
			}
		}

		idxnew = append(idxnew, append(append(skv.IsetIndexFieldPrefix(key, siKey), siEntry.Data...), prikey...))
	}

	//
	wb := levigo.NewWriteBatch()
	defer wb.Close()

	// wo := levigo.NewWriteOptions()
	// defer wo.Close()

	for _, idxkey := range idxdup {
		wb.Delete(idxkey)
	}

	for _, idxkey := range idxnew {
		wb.Put(idxkey, []byte{})
	}

	bvalue, _ := skv.JsonEncode(set)
	wb.Put(bkey, bvalue)

	if err := db.ldb.Write(db.writeOpts, wb); err != nil {
		rpl.Status = err.Error()
	} else if len_incr {
		db._raw_incrby(skv.IsetLenKey(key), 1)
	}

	return rpl
}

func (db *DB) Idel(key, prikey []byte) *skv.Reply {

	_iset_global_locker.Lock()
	_iset_global_locker.Unlock()

	var (
		rpl     = skv.NewReply("")
		bkey    = skv.IsetEntryKey(key, prikey)
		previdx = map[uint8]skv.IsetEntryBytes{}
	)

	if rs := db._raw_get(bkey); rs.Status == skv.ReplyNotFound {

		return rpl

	} else if rs.Status != skv.ReplyOK {

		return rs

	} else {

		db._raw_incrby(skv.IsetLenKey(key), -1)

		var prev map[string]interface{}

		if err := rs.JsonDecode(&prev); err == nil {
			previdx = skv.IsetIndexDataExport(_iset_indexes, key, prev)
		}
	}

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	// wo := levigo.NewWriteOptions()
	// defer wo.Close()

	for piKey, piEntry := range previdx {
		wb.Delete(append(append(skv.IsetIndexFieldPrefix(key, piKey), piEntry.Data...), prikey...))
	}

	wb.Delete(bkey)

	if err := db.ldb.Write(db.writeOpts, wb); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Ilen(key []byte) *skv.Reply {
	return db._raw_get(skv.IsetLenKey(key))
}
