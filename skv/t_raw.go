// Copyright 2015-2016 lessdb Author, All rights reserved.
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
	"github.com/lessos/lessdb/dbutil"
)

type RawInterface interface {
	RawGet(key []byte) *Reply
	RawPut(key, value []byte, ttl int64) *Reply
	RawDel(key []byte, value interface{}, ttl int64) *Reply
	RawScan(keys ...[]byte) *Reply
	RawRevScan(cursor, end []byte, limit uint32) *Reply
	RawIncrby(key []byte, step int64) *Reply
}

func RawNsKeyConcat(ns byte, key []byte) []byte {
	return append([]byte{ns}, keyLenFilter(key)...)
}

func RawNsKeyEncode(ns byte, key []byte) []byte {

	k := keyLenFilter(key)

	return append([]byte{ns, uint8(len(k))}, k...)
}

func RawTtlEntry(key []byte) []byte {
	return RawNsKeyConcat(NsRawTtlEntry, key)
}

func RawTtlQueuePrefix(ttlat uint64) []byte {
	return RawNsKeyConcat(NsRawTtlQueue, dbutil.Uint64ToBytes(ttlat))
}

func RawTtlQueue(key []byte, ttlat uint64) []byte {
	return append(RawTtlQueuePrefix(ttlat), key...)
}
