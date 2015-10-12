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
	"encoding/binary"
	"strconv"
	"sync"

	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	_raw_incr_locker sync.Mutex
)

func (db *DB) _raw_get(key []byte) *skv.Reply {

	rpl := skv.NewReply("")

	if data, err := db.ldb.Get(key, nil); err != nil {

		if err.Error() == "leveldb: not found" {
			rpl.Status = skv.ReplyNotFound
		} else {
			rpl.Status = err.Error()
		}

	} else {
		rpl.Data = [][]byte{data}
	}

	return rpl
}

func (db *DB) _raw_put(key, value []byte, ttl uint32) *skv.Reply {

	rpl := skv.NewReply("")

	if len(key) < 2 {
		rpl.Status = skv.ReplyInvalidArgument
		return rpl
	}

	if ttl > 0 {

		if ttl < 300 {
			return rpl
		}

		switch key[0] {
		case skv.NsKvEntry:
			if ok := db._raw_ssttl_put(key[0], key[1:], ttl); !ok {
				rpl.Status = skv.ReplyInvalidArgument
				return rpl
			}
		default:
			rpl.Status = skv.ReplyInvalidArgument
			return rpl
		}
	}

	if err := db.ldb.Put(key, value, nil); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) _raw_put_json(key []byte, value interface{}, ttl uint32) *skv.Reply {

	bvalue, err := skv.JsonEncode(value)
	if err != nil {
		return skv.NewReply(err.Error())
	}

	return db._raw_put(key, bvalue, ttl)
}

func (db *DB) _raw_del(keys ...[]byte) *skv.Reply {

	rpl := skv.NewReply("")

	batch := new(leveldb.Batch)

	for _, key := range keys {
		batch.Delete(key)
	}

	if err := db.ldb.Write(batch, nil); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) _raw_scan(cursor, end []byte, limit uint64) *skv.Reply {

	rpl := skv.NewReply("")

	if len(end) < 1 {
		end = cursor
	}

	for i := len(end); i < 255; i++ {
		end = append(end, 0xff)
	}

	if limit > skv.ScanLimitMax {
		limit = skv.ScanLimitMax
	}

	iter := db.ldb.NewIterator(&util.Range{Start: cursor, Limit: end}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Key()))
		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) _raw_revscan(cursor, end []byte, limit uint64) *skv.Reply {

	rpl := skv.NewReply("")

	if len(cursor) < 1 {
		cursor = end
	}

	for i := len(cursor); i < 256; i++ {
		cursor = append(cursor, 0x00)
	}

	for i := len(end); i < 256; i++ {
		end = append(end, 0xff)
	}

	if limit < skv.ScanLimitMax {
		limit = skv.ScanLimitMax
	}

	iter := db.ldb.NewIterator(&util.Range{Start: cursor, Limit: end}, nil)

	for ok := iter.Last(); ok; ok = iter.Prev() {

		if limit < 1 {
			break
		}

		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Key()))
		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) _raw_incrby(key []byte, step int64) *skv.Reply {

	if step == 0 {
		return skv.NewReply("")
	}

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
	rpl := db._raw_put(key, bnum, 0)
	if rpl.Status == skv.ReplyOK {
		rpl.Data = append(rpl.Data, bnum)
	}

	return rpl
}

func (db *DB) _raw_ssttl_get(ns byte, key []byte) *skv.Reply {

	key = skv.RawNsKeyConcat(ns, key)

	ttl := db._raw_get(skv.RawTtlEntry(key)).Int64() - int64(skv.TimeNowMS())
	if ttl < 0 {
		ttl = 0
	}

	rpl := skv.NewReply("")

	rpl.Data = append(rpl.Data, []byte(strconv.FormatInt(ttl, 10)))

	return rpl
}

func (db *DB) _raw_ssttl_put(ns byte, key []byte, ttl uint32) bool {

	key = skv.RawNsKeyConcat(ns, key)

	if ttl > 1000 {

		tto := skv.TimeNowMS() + uint64(ttl)

		batch := new(leveldb.Batch)

		//
		if prev := db._raw_get(skv.RawTtlEntry(key)); prev.Status == skv.ReplyOK && prev.Uint64() != tto {
			batch.Delete(skv.RawTtlQueue(key, prev.Uint64()))
		}

		//
		batch.Put(skv.RawTtlQueue(key, tto), []byte{})

		//
		batch.Put(skv.RawTtlEntry(key), []byte(strconv.FormatUint(tto, 10)))

		if err := db.ldb.Write(batch, nil); err != nil {
			return false
		}
	}

	return true
}

func (db *DB) _raw_ssttl_range(score_start, score_end, limit uint64) *skv.Reply {

	var (
		bs_start = skv.RawTtlQueuePrefix(score_start)
		bs_end   = skv.RawTtlQueuePrefix(score_end)
		rpl      = skv.NewReply("")
	)

	for i := len(bs_end); i < 256; i++ {
		bs_end = append(bs_end, 0xff)
	}

	iter := db.ldb.NewIterator(&util.Range{Start: bs_start, Limit: bs_end}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		if len(iter.Key()) < 10 {
			db._raw_del(iter.Key())
			continue
		}

		ui64 := binary.BigEndian.Uint64(iter.Key()[1:9])

		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Key()))
		rpl.Data = append(rpl.Data, []byte(strconv.FormatUint(ui64, 10)))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}
