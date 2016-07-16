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

package dbtypes

import (
	"testing"

	"github.com/lessos/lessgo/crypto/idhash"
)

var (
	gbit2  = NewBitmap(2)
	gbit4  = NewBitmap(4)
	gbit8  = NewBitmap(8)
	glimit = 100000
	gkeys  = [][]byte{}
)

func init() {

	for i := 0; i < glimit; i++ {
		gkeys = append(gkeys, idhash.Rand(8))
	}
}

func TestBitmap4(t *testing.T) {

	bit4 := NewBitmap(4)
	keys := [][]byte{}

	for i := 0; i < 10000; i++ {

		key := idhash.Rand(4)
		bit4.Set(key)

		keys = append(keys, key)
	}

	for _, key := range keys {

		if !bit4.Has(key) {
			t.Fatal("Failed on Bitmap.Has()")
		}

		bit4.Del(key)
	}

	for _, key := range keys {

		if bit4.Has(key) {
			t.Fatal("Failed on Bitmap.Has()")
		}
	}
}

func TestBitmap8(t *testing.T) {

	bit8 := NewBitmap(8)
	keys := [][]byte{}

	for i := 0; i < 10000; i++ {

		key := idhash.Rand(8)
		bit8.Set(key)

		keys = append(keys, key)
	}

	for _, key := range keys {

		if !bit8.Has(key) {
			t.Fatal("Failed on Bitmap.Has()")
		}

		bit8.Del(key)
	}

	for _, key := range keys {

		if bit8.Has(key) {
			t.Fatal("Failed on Bitmap.Has()")
		}
	}
}

func Benchmark_Bitmap2Set(b *testing.B) {

	for i := 0; i < b.N; i++ {
		gbit2.Set(gkeys[i%glimit][:2])
	}
}

func Benchmark_Bitmap4Set(b *testing.B) {

	for i := 0; i < b.N; i++ {
		gbit4.Set(gkeys[i%glimit][:4])
	}
}

func Benchmark_Bitmap8Set(b *testing.B) {

	for i := 0; i < b.N; i++ {
		gbit8.Set(gkeys[i%glimit])
	}
}
