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

func _set_key(key []byte) []byte {
	return append([]byte{ns_set_entry}, key...)
}

func (db *DB) Scan(cursor, end []byte, limit uint64) *Reply {
	return db._raw_scan(_set_key(cursor), _set_key(end), limit)
}

func (db *DB) SetJson(key []byte, value interface{}) *Reply {
	return db._raw_set_json(_set_key(key), value)
}

func (db *DB) Set(key, value []byte) *Reply {
	return db._raw_set(_set_key(key), value)
}

func (db *DB) Setex(key, value []byte, ttl uint64) *Reply {
	return db._raw_setex(_set_key(key), value, ttl)
}

func (db *DB) Incr(key []byte, step int64) *Reply {
	return db._raw_incr(_set_key(key), step)
}

func (db *DB) Get(key []byte) *Reply {
	return db._raw_get(_set_key(key))
}

func (db *DB) Del(keys ...[]byte) *Reply {

	for k, v := range keys {
		keys[k] = _set_key(v)
	}

	return db._raw_del(keys...)
}
