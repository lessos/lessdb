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
	"crypto/sha1"
	"encoding/binary"
	"io"
	"reflect"
	"strconv"
	"strings"
)

func ObjectDocFoldKey(fold string) []byte {
	return _obj_str_hash(ObjectPathClean(fold), _obj_fold_len)
}

func ObjectDocSchemaKey(key []byte) []byte {
	return RawKeyEncode(ns_object_doc_schema, key)
}

func _object_doc_idx_key_prefix(key []byte) []byte {
	return RawKeyEncode(ns_object_doc_index, key)
}

func ObjectDocIndexFieldPrefix(key []byte, column uint8) []byte {
	return append(_object_doc_idx_key_prefix(key), column)
}

func ObjectDocIndexIncrKey(key []byte, column uint8) []byte {
	return append(RawKeyEncode(ns_object_doc_increment, key), column)
}

func ObjectDocBytesIncr(key []byte) []byte {

	for i := (len(key) - 1); i >= 0; i-- {

		if key[i] == 255 {
			key[i] = 0
		} else {
			key[i] = key[i] + 1
			break
		}
	}

	return key
}

func ObjectDocBytesDecr(key []byte) []byte {

	for i := (len(key) - 1); i >= 0; i-- {

		if key[i] == 0 {
			key[i] = 255
		} else {
			key[i] = key[i] - 1
			break
		}
	}

	return key
}

func ObjectDocIndexRawKeyExport(data []byte, ilen uint8) ([]byte, []byte, bool) {

	if len(data) < 7 {
		return []byte{}, []byte{}, false
	}

	kplen := int(data[1] + 3)

	pken := kplen + int(ilen)

	if len(data) <= pken {
		return []byte{}, []byte{}, false
	}

	return data[kplen:pken], data[pken:], true
}

func ObjectDocBytesToUint64(key []byte) uint64 {

	if len(key) < 1 || len(key) > 8 {
		return 0
	}

	uibs := make([]byte, 8)

	offset := 8 - len(key)

	for i := 0; i < len(key); i++ {
		uibs[i+offset] = key[i]
	}

	return binary.BigEndian.Uint64(uibs)
}

func ObjectDocIndexStringFilter(key string) string {
	return strings.ToLower(strings.TrimSpace(key))
}

func _object_doc_idx_string_to_bytes(key string) []byte {

	h := sha1.New()
	io.WriteString(h, key)

	return h.Sum(nil)
}

func ObjectDocIndexSintToBytes(sint string, lg uint8) []byte {

	if lg < 1 {
		lg = 1
	} else if lg > 8 {
		lg = 8
	}

	ui64, _ := strconv.ParseUint(sint, 10, 64)

	uibs := make([]byte, 8)

	binary.BigEndian.PutUint64(uibs, ui64)

	return uibs[8-lg:]
}

func ObjectDocIndexValue(idx *ObjectDocSchemaIndexEntry, value reflect.Value) ([]byte, bool) {

	ui64, bs := uint64(0), []byte{}

	if value.Kind() == reflect.Interface {
		value = reflect.ValueOf(value.Interface())
	}

	switch value.Kind() {

	// []byte
	case reflect.Slice:
		bs = value.Bytes()

	// uint*
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		ui64 = value.Uint()

	// int*
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if i64 := value.Int(); i64 >= 0 {
			ui64 = uint64(i64)
		}

	// float*
	case reflect.Float32, reflect.Float64:
		if f64 := value.Float(); f64 >= 0 {
			ui64 = uint64(f64)
		}

	// bool
	case reflect.Bool:
		if value.Bool() {
			ui64 = 1
		}

	// string
	case reflect.String:
		if idx.Type == ObjectDocSchemaIndexTypeUint {
			ui64, _ = strconv.ParseUint(value.String(), 10, 64)
		} else {
			bs = _object_doc_idx_string_to_bytes(ObjectDocIndexStringFilter(value.String()))
		}

	//
	default:
		return bs, false
	}

	//
	switch idx.Type {

	case ObjectDocSchemaIndexTypeUint:

		ibs := make([]byte, 8)
		binary.BigEndian.PutUint64(ibs, ui64)

		if idx.Length > 8 {
			idx.Length = 8
		} else if idx.Length < 1 {
			idx.Length = 1
		}

		bs = ibs[(8 - idx.Length):]

	case ObjectDocSchemaIndexTypeString, ObjectDocSchemaIndexTypeBinary:

		if idx.Length > 16 {
			idx.Length = 16
		} else if idx.Length < 1 {
			idx.Length = 1
		}

		if len(bs) > int(idx.Length) {
			bs = bs[:idx.Length]
		}

	default:
		return bs, false
	}

	return bs, (len(bs) == int(idx.Length))
}
