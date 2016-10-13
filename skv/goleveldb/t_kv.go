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
)

func (db *DB) KvGet(key []byte) *skv.Reply {
	return db.RawGet(skv.RawNsKeyConcat(skv.NsKvEntry, key))
}

func (db *DB) KvPut(key, value []byte, ttl int64) *skv.Reply {
	return db.RawPut(skv.RawNsKeyConcat(skv.NsKvEntry, key), value, ttl)
}

func (db *DB) KvPutJson(key []byte, value interface{}, ttl int64) *skv.Reply {
	return db._raw_put_json(skv.RawNsKeyConcat(skv.NsKvEntry, key), value, ttl)
}

func (db *DB) KvScan(cursor, end []byte, limit uint32) *skv.Reply {

	rpl := db.RawScan(skv.RawNsKeyConcat(skv.NsKvEntry, cursor), skv.RawNsKeyConcat(skv.NsKvEntry, end), limit)

	if len(rpl.Data) > 0 && len(rpl.Data)%2 == 0 {
		for i := 0; i < len(rpl.Data); i += 2 {
			rpl.Data[i] = rpl.Data[i][1:]
		}
	}

	return rpl
}

func (db *DB) KvDel(keys ...[]byte) *skv.Reply {

	for k, v := range keys {
		keys[k] = skv.RawNsKeyConcat(skv.NsKvEntry, v)
	}

	return db._raw_del(keys...)
}

func (db *DB) KvIncrby(key []byte, step int64) *skv.Reply {
	return db._raw_incrby(skv.RawNsKeyConcat(skv.NsKvEntry, key), step)
}

func (db *DB) KvTtl(key []byte) *skv.Reply {
	return db._raw_ssttl_get(skv.NsKvEntry, key)
}
