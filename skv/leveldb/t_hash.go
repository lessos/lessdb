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

	"github.com/jmhodges/levigo"
	"github.com/lessos/lessdb/skv"
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

func (db *DB) Hget(key, field []byte) *skv.Reply {
	return db._raw_get(_hset_key(key, field))
}

func (db *DB) Hscan(key, cursor, end []byte, limit uint64) *skv.Reply {

	if limit > scan_max_limit {
		limit = scan_max_limit
	}

	var (
		prefix = _hset_key_prefix(key)
		prelen = len(prefix)
		cstart = append(prefix, cursor...)
		cend   = append(prefix, end...)
		rpl    = skv.NewReply("")
	)

	for i := len(cend); i < 256; i++ {
		cend = append(cend, 0xff)
	}

	ro := levigo.NewReadOptions()
	ro.SetFillCache(false)
	defer ro.Close()

	it := db.ldb.NewIterator(ro)
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

		rpl.Data = append(rpl.Data, bytesClone(it.Key()[prelen:]))
		rpl.Data = append(rpl.Data, bytesClone(it.Value()))

		limit--
	}

	if err := it.GetError(); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Hset(key, field, value []byte, ttl uint64) *skv.Reply {

	bkey := _hset_key(key, field)

	if rs := db._raw_get(bkey); rs.Status == skv.ReplyNotFound {
		db._raw_incrby(_hlen_key(key), 1)
	}

	return db._raw_set(bkey, value, 0)
}

func (db *DB) HsetJson(key, field []byte, value interface{}, ttl uint64) *skv.Reply {

	bkey := _hset_key(key, field)

	if rs := db._raw_get(bkey); rs.Status == skv.ReplyNotFound {
		db._raw_incrby(_hlen_key(key), 1)
	}

	return db._raw_set_json(bkey, value, 0)
}

func (db *DB) Hdel(key, field []byte) *skv.Reply {

	bkey := _hset_key(key, field)
	rpl := skv.NewReply("")

	if rs := db._raw_get(bkey); rs.Status == skv.ReplyOK {
		db._raw_incrby(_hlen_key(key), -1)
		rpl = db._raw_del(bkey)
	}

	return rpl
}

func (db *DB) Hlen(key []byte) *skv.Reply {
	return db._raw_get(_hlen_key(key))
}