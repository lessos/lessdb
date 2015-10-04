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

import (
	"encoding/binary"
)

func RawKeyEncode(ns byte, key []byte) []byte {

	si := len(key)
	if si > 255 {
		si = 255
		key = key[:255]
	}

	return append([]byte{ns, uint8(si)}, key...)
}

func RawTtlEntry(key []byte) []byte {
	return RawKeyEncode(NsRawTtlEntry, key)
}

func RawTtlQueuePrefix(ttl uint64) []byte {

	bscore := make([]byte, 8)
	binary.BigEndian.PutUint64(bscore, ttl)

	return append([]byte{NsRawTtlQueue}, bscore...)
}

func RawTtlQueue(key []byte, ttl uint64) []byte {

	bscore := make([]byte, 8)
	binary.BigEndian.PutUint64(bscore, ttl)

	return append(append([]byte{NsRawTtlQueue}, bscore...), key...)
}
