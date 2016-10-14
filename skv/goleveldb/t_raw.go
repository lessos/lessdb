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
	"time"

	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	_raw_incr_locker sync.Mutex
)

func (db *DB) RawGet(key []byte) *skv.Reply {

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

func (db *DB) RawPut(key, value []byte, ttl int64) *skv.Reply {

	rpl := skv.NewReply("")

	if len(key) < 2 {
		rpl.Status = skv.ReplyBadArgument
		return rpl
	}

	if ttl > 0 {

		if ttl < 1000 {
			return rpl
		}

		switch key[0] {
		case skv.NsKvEntry:
			if ok := db._raw_ssttlat_put(key[0], key[1:], skv.MetaTimeNowAddMS(ttl)); !ok {
				rpl.Status = skv.ReplyBadArgument
				return rpl
			}
		default:
			rpl.Status = skv.ReplyBadArgument
			return rpl
		}
	}

	if err := db.ldb.Put(key, value, nil); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) _raw_put_json(key []byte, value interface{}, ttl int64) *skv.Reply {

	bvalue, err := skv.JsonEncode(value)
	if err != nil {
		return skv.NewReply(err.Error())
	}

	return db.RawPut(key, bvalue, ttl)
}

func (db *DB) RawDel(keys ...[]byte) *skv.Reply {

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

func (db *DB) RawScan(cursor, end []byte, limit uint32) *skv.Reply {

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

func (db *DB) RawRevScan(cursor, end []byte, limit uint32) *skv.Reply {

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

	if limit > skv.ScanLimitMax {
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

	rpl := db.RawGet(key)
	if rpl.Status == skv.ReplyOK {
		num = rpl.Uint64()
	}

	if step == 0 {
		rpl.Data = append(rpl.Data, []byte(strconv.FormatUint(num, 10)))
		rpl.Status = skv.ReplyOK
		return rpl
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
	rpl = db.RawPut(key, bnum, 0)
	if rpl.Status == skv.ReplyOK {
		rpl.Data = append(rpl.Data, bnum)
	}

	return rpl
}

func (db *DB) _raw_ssttl_get(ns byte, key []byte) *skv.Reply {

	key = skv.RawNsKeyConcat(ns, key)

	rpl, ttl := skv.NewReply(""), int64(0)

	if ttlat := skv.BytesToUint64(db.RawGet(skv.RawTtlEntry(key)).Bytes()); ttlat > 0 {
		ttl = (skv.MetaTimeParse(ttlat).UnixNano() - time.Now().UTC().UnixNano()) / 1e6
	}

	if ttl < 0 {
		ttl = 0
	}

	rpl.Data = append(rpl.Data, []byte(strconv.FormatInt(ttl, 10)))

	return rpl
}

func (db *DB) _raw_ssttlat_put(ns byte, key []byte, ttlat uint64) bool {

	if ttlat == 0 {
		return true
	}

	key = skv.RawNsKeyConcat(ns, key)

	batch := new(leveldb.Batch)

	//
	if prev := db.RawGet(skv.RawTtlEntry(key)); prev.Status == skv.ReplyOK {
		if prev_ttlat := skv.BytesToUint64(prev.Bytes()); prev_ttlat != ttlat {
			batch.Delete(skv.RawTtlQueue(key, prev_ttlat))
		}
	}

	//
	batch.Put(skv.RawTtlQueue(key, ttlat), []byte{})

	//
	batch.Put(skv.RawTtlEntry(key), skv.Uint64ToBytes(ttlat))

	if err := db.ldb.Write(batch, nil); err != nil {
		return false
	}

	return true
}

func (db *DB) _raw_ssttlat_range(score_start, score_end, limit uint64) *skv.Reply {

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
			db.RawDel(iter.Key())
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
