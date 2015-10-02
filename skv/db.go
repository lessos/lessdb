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

const (
	//
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024

	//
	TtlWorkerSleep        = 300e6
	TtlWorkerLimit uint64 = 10000
	ScanMaxLimit   uint64 = 10000

	//
	ObjectMetaBytes     = 0x00
	ObjectMetaJson      = 0x01
	ObjectMetaIndexJson = 0x02

	//
	ns_zero           = 0x00
	ns_raw_ttl        = 0x79
	ns_set_entry      = 0x80
	ns_hash_entry     = 0x81
	ns_hash_len       = 0x82
	ns_zset_entry     = 0x83
	ns_zset_score     = 0x84
	ns_zset_length    = 0x85
	ns_iset_schema    = 0x86
	ns_iset_entry     = 0x87
	ns_iset_index     = 0x88
	ns_iset_length    = 0x89
	ns_iset_increment = 0x90
	ns_object_meta    = 0xa0
	ns_object_entry   = 0xa1
)

var (
	ns_set_ttl = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00}
)

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

	// Indexed Key JSON APIs
	IschemaSet(key []byte, schema IsetSchema) *Reply
	Iget(key, prikey []byte) *Reply
	Iset(key, prikey []byte, obj interface{}) *Reply
	Idel(key, prikey []byte) *Reply
	Iscan(key, cursor, end []byte, limit uint64) *Reply
	Iquery(key []byte, qry *QuerySet) *Reply
	Ilen(key []byte) *Reply

	// Client APIs
	Close()
}
