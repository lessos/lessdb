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
	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (db *DB) HashGet(key, field []byte) *skv.Reply {
	return db.RawGet(skv.HashNsEntryKey(key, field))
}

func (db *DB) HashPut(key, field, value []byte, ttl int64) *skv.Reply {

	bkey := skv.HashNsEntryKey(key, field)

	if rs := db.RawGet(bkey); rs.Status == skv.ReplyNotFound {
		db._raw_incrby(skv.HashNsLengthKey(key), 1)
	}

	return db.RawPut(bkey, value, 0)
}

func (db *DB) HashPutJson(key, field []byte, value interface{}, ttl int64) *skv.Reply {

	bkey := skv.HashNsEntryKey(key, field)

	if rs := db.RawGet(bkey); rs.Status == skv.ReplyNotFound {
		db._raw_incrby(skv.HashNsLengthKey(key), 1)
	}

	return db._raw_put_json(bkey, value, 0)
}

func (db *DB) HashDel(key, field []byte) *skv.Reply {

	bkey := skv.HashNsEntryKey(key, field)

	if rs := db.RawGet(bkey); rs.Status == skv.ReplyOK {
		db._raw_incrby(skv.HashNsLengthKey(key), -1)
		return db._raw_del(bkey)
	}

	return skv.NewReply("")
}

func (db *DB) HashScan(key, cursor, end []byte, limit uint32) *skv.Reply {

	var (
		prefix = skv.RawNsKeyEncode(skv.NsHashEntry, key)
		prelen = len(prefix)
		cstart = append(prefix, cursor...)
		cend   = append(prefix, end...)
		rpl    = skv.NewReply("")
	)

	for i := len(cend); i < 256; i++ {
		cend = append(cend, 0xff)
	}

	if limit > skv.ScanLimitMax {
		limit = skv.ScanLimitMax
	}

	iter := db.ldb.NewIterator(&util.Range{Start: cstart, Limit: append(cend)}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		if len(iter.Key()) < prelen {
			continue
		}

		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Key()[prelen:]))
		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) HashLen(key []byte) *skv.Reply {
	return db.RawGet(skv.HashNsLengthKey(key))
}
