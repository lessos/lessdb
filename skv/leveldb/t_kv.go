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
	"github.com/lessos/lessdb/skv"
)

func _set_key(key []byte) []byte {
	return append([]byte{ns_set_entry}, key...)
}

func (db *DB) Scan(cursor, end []byte, limit uint64) *skv.Reply {

	rpl := db._raw_scan(_set_key(cursor), _set_key(end), limit)

	if len(rpl.Data) > 0 && len(rpl.Data)%2 == 0 {
		for i := 0; i < len(rpl.Data); i += 2 {
			rpl.Data[i] = rpl.Data[i][1:]
		}
	}

	return rpl
}

func (db *DB) SetJson(key []byte, value interface{}, ttl uint64) *skv.Reply {
	return db._raw_set_json(_set_key(key), value, ttl)
}

func (db *DB) Set(key, value []byte, ttl uint64) *skv.Reply {
	return db._raw_set(_set_key(key), value, ttl)
}

func (db *DB) Incrby(key []byte, step int64) *skv.Reply {
	return db._raw_incrby(_set_key(key), step)
}

func (db *DB) Get(key []byte) *skv.Reply {
	return db._raw_get(_set_key(key))
}

func (db *DB) Del(keys ...[]byte) *skv.Reply {

	for k, v := range keys {
		keys[k] = _set_key(v)
	}

	return db._raw_del(keys...)
}

func (db *DB) Ttl(key []byte) *skv.Reply {
	return db._raw_ttl(_set_key(key))
}
