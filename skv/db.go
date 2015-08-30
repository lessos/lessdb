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

type DB interface {
	// Raw
	// RawScan(cursor, end []byte, limit uint64) *Reply

	// General Key Value APIs
	Get(key []byte) *Reply
	Set(key, value []byte, ttl uint64) *Reply
	SetJson(key []byte, value interface{}, ttl uint64) *Reply
	Del(keys ...[]byte) *Reply
	Scan(cursor, end []byte, limit uint64) *Reply
	Incrby(key []byte, step int64) *Reply
	Ttl(key []byte) *Reply

	// Hash Key Value APIs
	Hget(key, field []byte) *Reply
	Hset(key, field, value []byte, ttl uint64) *Reply
	HsetJson(key, field []byte, value interface{}, ttl uint64) *Reply
	Hdel(key, field []byte) *Reply
	Hscan(key, cursor, end []byte, limit uint64) *Reply
	Hlen(key []byte) *Reply

	// Sorted Key Value APIs
	Zget(key, member []byte) *Reply
	Zset(key, member []byte, score uint64) *Reply
	Zdel(key, member []byte) *Reply
	Zrange(key []byte, score_start, score_end, limit uint64) *Reply
	Zlen(key []byte) *Reply

	// Client APIs
	Close()
}
