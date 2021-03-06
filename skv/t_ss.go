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

type SsInterface interface {
	SsGet(key, member []byte) *Reply
	SsPut(key, member []byte, score uint64) *Reply
	SsDel(key, member []byte) *Reply
	SsRange(key []byte, score_start, score_end uint64, limit uint32) *Reply
	SsLen(key []byte) *Reply
}

func SortSetsNsEntryKey(key, member []byte) []byte {
	return append(RawNsKeyEncode(nsSsEntry, key), member...)
}

func SortSetsNsLengthKey(key []byte) []byte {
	return RawNsKeyConcat(nsSsLength, key)
}

func SortSetsNsScorePrefix(key []byte, score uint64) []byte {
	return append(RawNsKeyEncode(nsSsScore, key), dbutil.Uint64ToBytes(score)...)
}

func SortSetsNsScoreKey(key, member []byte, score uint64) []byte {
	return append(SortSetsNsScorePrefix(key, score), member...)
}
