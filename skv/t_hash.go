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

type HashInterface interface {
	HashGet(key, field []byte) *Reply
	HashPut(key, field, value []byte, ttl int64) *Reply
	HashPutJson(key, field []byte, value interface{}, ttl int64) *Reply
	HashDel(key, field []byte) *Reply
	HashScan(key, cursor, end []byte, limit uint32) *Reply
	HashLen(key []byte) *Reply
}

func HashNsEntryKey(key, field []byte) []byte {
	return append(RawNsKeyEncode(NsHashEntry, key), field...)
}

func HashNsLengthKey(key []byte) []byte {
	return RawNsKeyConcat(NsHashLength, key)
}
