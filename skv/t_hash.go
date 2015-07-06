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
	"github.com/syndtr/goleveldb/leveldb/util"
)

func _hset_key_prefix(key []byte) []byte {

	si := len(key)
	if si > 255 {
		si = 255
	}

	return append([]byte{ns_hash_entry, uint8(si)}, key...)
}

func _hset_key(key, field []byte) []byte {
	return append(_hset_key_prefix(key), field...)
}

func _hlen_key(key []byte) []byte {

	si := len(key)
	if si > 255 {
		si = 255
	}

	return append([]byte{ns_hash_len, uint8(si)}, key...)
}

func (db *DB) Hget(key, field []byte) *Reply {
	return db._raw_get(_hset_key(key, field))
}

func (db *DB) Hscan(key, cursor, end []byte, limit uint64) *Reply {

	if len(end) < 1 {
		end = cursor
	}

	if limit < scan_max_limit {
		limit = scan_max_limit
	}

	var (
		prefix = _hset_key_prefix(key)
		prelen = len(prefix)
		cstart = append(prefix, cursor...)
		cend   = append(prefix, end...)
		rpl    = NewReply("")
	)

	iter := db.ldb.NewIterator(&util.Range{Start: cstart, Limit: append(cend)}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		if len(iter.Key()) < prelen {
			continue
		}

		rpl.Data = append(rpl.Data, bytesClone(iter.Key()[prelen:]))
		rpl.Data = append(rpl.Data, bytesClone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) Hset(key, field, value []byte) *Reply {

	bkey := _hset_key(key, field)

	if rs := db._raw_get(bkey); rs.Status == ReplyNotFound {
		db._raw_incr(_hlen_key(key), 1)
	}

	return db._raw_set(bkey, value)
}

func (db *DB) Hdel(key, field []byte) *Reply {

	bkey := _hset_key(key, field)
	rpl := NewReply("")

	if rs := db._raw_get(bkey); rs.Status == ReplyOK {
		db._raw_incr(_hlen_key(key), -1)
		rpl = db._raw_del(bkey)
	}

	return rpl
}

func (db *DB) Hlen(key []byte) *Reply {
	return db._raw_get(_hlen_key(key))
}
