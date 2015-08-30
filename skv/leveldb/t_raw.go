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
	"strconv"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/lessos/lessdb/skv"
)

var (
	_raw_incr_locker sync.Mutex
)

func _raw_key_encode(ns byte, key []byte) []byte {

	si := len(key)
	if si > 255 {
		si = 255
		key = key[:255]
	}

	return append([]byte{ns, uint8(si)}, key...)
}

func (db *DB) RawScan(cursor, end []byte, limit uint64) *skv.Reply {
	return db._raw_scan(cursor, end, limit)
}

func (db *DB) _raw_scan(cursor, end []byte, limit uint64) *skv.Reply {

	rpl := skv.NewReply("")

	if len(end) < 1 {
		end = cursor
	}

	for i := len(end); i < 255; i++ {
		end = append(end, 0xff)
	}

	if limit > scan_max_limit {
		limit = scan_max_limit
	}

	ro := levigo.NewReadOptions()
	ro.SetFillCache(false)
	defer ro.Close()

	it := db.ldb.NewIterator(ro)
	defer it.Close()

	for it.Seek(cursor); it.Valid(); it.Next() {

		if limit < 1 {
			break
		}

		if bytes.Compare(it.Key(), end) > 0 {
			break
		}

		rpl.Data = append(rpl.Data, bytesClone(it.Key()))
		rpl.Data = append(rpl.Data, bytesClone(it.Value()))

		limit--
	}

	if err := it.GetError(); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) _raw_revscan(cursor, end []byte, limit uint64) *skv.Reply {

	rpl := skv.NewReply("")

	if len(cursor) < 1 {
		cursor = end
	}

	for i := len(cursor); i < 256; i++ {
		cursor = append(cursor, 0xff)
	}

	for i := len(end); i < 256; i++ {
		end = append(end, 0x00)
	}

	if limit < scan_max_limit {
		limit = scan_max_limit
	}

	ro := levigo.NewReadOptions()
	ro.SetFillCache(false)
	defer ro.Close()

	it := db.ldb.NewIterator(ro)
	defer it.Close()

	for it.Seek(cursor); it.Valid(); it.Prev() {

		if limit < 1 {
			break
		}

		if bytes.Compare(it.Key(), end) < 0 {
			break
		}

		rpl.Data = append(rpl.Data, bytesClone(it.Key()))
		rpl.Data = append(rpl.Data, bytesClone(it.Value()))

		limit--
	}

	if err := it.GetError(); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) _raw_set_json(key []byte, value interface{}, ttl uint64) *skv.Reply {

	bvalue, err := jsonEncode(value)
	if err != nil {
		return skv.NewReply(err.Error())
	}

	return db._raw_set(key, bvalue, ttl)
}

func (db *DB) _raw_set(key, value []byte, ttl uint64) *skv.Reply {

	rpl := skv.NewReply("")

	if ttl > 0 {

		if ttl < 300 {
			return rpl
		}

		rpl = db.Zset(ns_set_ttl, key, timeNowMS()+ttl)
		if rpl.Status != skv.ReplyOK {
			return rpl
		}
	}

	wo := levigo.NewWriteOptions()
	defer wo.Close()

	if err := db.ldb.Put(wo, key, value); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) _raw_ttl(key []byte) *skv.Reply {

	ttl := db.Zget(ns_set_ttl, key).Int64() - int64(timeNowMS())
	if ttl < 0 {
		ttl = -1
	}

	rpl := skv.NewReply("")

	rpl.Data = append(rpl.Data, []byte(strconv.FormatInt(ttl, 10)))

	return rpl
}

func (db *DB) _raw_incrby(key []byte, step int64) *skv.Reply {

	_raw_incr_locker.Lock()
	defer _raw_incr_locker.Unlock()

	num := uint64(0)

	if rs := db._raw_get(key); rs.Status == skv.ReplyOK {
		num = rs.Uint64()
	}

	if step < 0 {

		if uint64(-step) > num {
			num = 0
		} else {
			num = num - uint64(-step)
		}

	} else {
		num += uint64(step)
	}

	bnum := []byte(strconv.FormatUint(num, 10))
	rpl := db._raw_set(key, bnum, 0)
	if rpl.Status == skv.ReplyOK {
		rpl.Data = append(rpl.Data, bnum)
	}

	return rpl
}

func (db *DB) _raw_get(key []byte) *skv.Reply {

	rpl := skv.NewReply("")

	ro := levigo.NewReadOptions()
	defer ro.Close()

	if data, err := db.ldb.Get(ro, key); err != nil || data == nil {

		if data == nil {
			rpl.Status = skv.ReplyNotFound
		} else {
			rpl.Status = err.Error()
		}

	} else {

		rpl.Data = [][]byte{data}
	}

	return rpl
}

func (db *DB) _raw_del(keys ...[]byte) *skv.Reply {

	rpl := skv.NewReply("")

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	wo := levigo.NewWriteOptions()
	defer wo.Close()

	for _, key := range keys {
		wb.Delete(key)
	}

	if err := db.ldb.Write(wo, wb); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}
