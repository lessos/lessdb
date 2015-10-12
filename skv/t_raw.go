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

func RawNsKeyConcat(ns byte, key []byte) []byte {
	return append([]byte{ns}, KeyLenFilter(key)...)
}

func RawNsKeyEncode(ns byte, key []byte) []byte {

	k2 := KeyLenFilter(key)

	return append([]byte{ns, uint8(len(k2))}, k2...)
}

func RawTtlEntry(key []byte) []byte {
	return RawNsKeyConcat(NsRawTtlEntry, key)
}

func RawTtlQueuePrefix(ttl uint64) []byte {
	return RawNsKeyConcat(NsRawTtlQueue, _uint64_to_bytes(ttl))
}

func RawTtlQueue(key []byte, ttl uint64) []byte {
	return append(RawTtlQueuePrefix(ttl), key...)
}
