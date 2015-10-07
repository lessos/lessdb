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
	NsRawTtlEntry           = 0x70
	NsRawTtlQueue           = 0x71
	NsKvEntry               = 0x80
	ns_hash_entry           = 0x81
	ns_hash_len             = 0x82
	ns_ss_entry             = 0x83
	ns_ss_score             = 0x84
	ns_ss_length            = 0x85
	ns_object_meta          = 0x90
	NsObjectEntry           = 0x91
	ns_object_doc_schema    = 0x92
	ns_object_doc_index     = 0x93
	ns_object_doc_increment = 0x94
)

type DB interface {
	// Key Value
	KvGet(key []byte) *Reply
	KvPut(key, value []byte, ttl uint32) *Reply
	KvPutJson(key []byte, value interface{}, ttl uint32) *Reply
	KvDel(keys ...[]byte) *Reply
	KvScan(cursor, end []byte, limit uint64) *Reply
	KvIncrby(key []byte, step int64) *Reply
	KvTtl(key []byte) *Reply

	// Hashs
	HashGet(key, field []byte) *Reply
	HashPut(key, field, value []byte, ttl uint32) *Reply
	HashPutJson(key, field []byte, value interface{}, ttl uint32) *Reply
	HashDel(key, field []byte) *Reply
	HashScan(key, cursor, end []byte, limit uint64) *Reply
	HashLen(key []byte) *Reply

	// Sorted Sets
	SsGet(key, member []byte) *Reply
	SsPut(key, member []byte, score uint64) *Reply
	SsDel(key, member []byte) *Reply
	SsRange(key []byte, score_start, score_end, limit uint64) *Reply
	SsLen(key []byte) *Reply

	// Client APIs
	Close()
}
