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

type Bitmap struct {
	data   []*bitmap_data
	keylen int
}

type bitmap_data struct {
	keylen int
	bytes  []byte
}

const (
	bitmap_size32 uint64 = 0x01 << 32
)

func NewBitmap(klen uint8) *Bitmap {

	if klen > 8 {
		klen = 8
	} else if klen < 1 {
		klen = 1
	}

	bmap := &Bitmap{
		keylen: int(klen),
	}

	if klen > 4 {

		bmap.data = append(bmap.data, &bitmap_data{
			keylen: 4,
			bytes:  make([]byte, uint64(bitmap_size32>>3)),
		})

		bmap.data = append(bmap.data, &bitmap_data{
			keylen: int(klen - 4),
			bytes:  make([]byte, uint64((0x01<<((klen-4)*8))>>3)),
		})

	} else {

		bmap.data = append(bmap.data, &bitmap_data{
			keylen: int(klen),
			bytes:  make([]byte, uint64((0x01<<(klen*8))>>3)),
		})
	}

	return bmap
}

func (bm *Bitmap) bin_index(key []byte) (bucket uint8, idx, pos uint64) {

	if len(key) > bm.keylen {
		key = key[:bm.keylen]
	}

	if len(key) != bm.keylen {
		return 0, 0, 0
	}

	ui64 := uint64(0)

	for i := 0; i < bm.keylen; i++ {

		if key[i] > 0 {
			ui64 += (uint64(key[i]) << (uint8(bm.keylen-i-1) * 8))
		}
	}

	boffset := ui64 % bitmap_size32
	if ui64 >= bitmap_size32 {
		return 1, boffset / 8, boffset % 8
	}

	return 0, boffset / 8, boffset % 8
}

func (bm *Bitmap) Set(key []byte) {

	bucket, idx, pos := bm.bin_index(key)

	bm.data[bucket].bytes[idx] |= 0x01 << pos
}

func (bm *Bitmap) Del(key []byte) {

	bucket, idx, pos := bm.bin_index(key)

	bm.data[bucket].bytes[idx] &^= 0x01 << pos
}

func (bm *Bitmap) Has(key []byte) bool {

	bucket, idx, pos := bm.bin_index(key)

	return ((bm.data[bucket].bytes[idx] >> pos) & 0x01) == 1
}

func (bm *Bitmap) Clean() {

	for i, v := range bm.data {
		bm.data[i].bytes = make([]byte, uint64((0x01<<uint(v.keylen*8))>>3))
	}
}
