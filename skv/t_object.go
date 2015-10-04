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
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"io"
	"path/filepath"
	"strings"
	// "fmt"
)

const (
	obj_fold_len   = 12
	obj_field_len  = 8
	obj_type_bytes = 0x00
	obj_type_json  = 0x01
	obj_type_index = 0x02
)

const (
	ObjectEventCreated = 0x01
	ObjectEventUpdated = 0x02
	ObjectEventDeleted = 0x03
)

type ObjectEventHandler func(string, uint64, uint16)

type ObjectInterface interface {
	ObjectEventRegister(ev ObjectEventHandler)
	//
	ObjectGet(path string) *Reply
	ObjectSet(path string, value []byte, ttl uint32) *Reply
	// ObjectSetJson(path string, value interface{}, ttl uint64) *Reply
	ObjectDel(path string) *Reply
	ObjectScan(path, cursor, end string, limit uint32) *Reply
	// ObjectSchemaSync(key []byte, schema IsetSchema) *Reply
	// ObjectQuery(key []byte, qry *QuerySet) *Reply
	ObjectMetaGet(path string) *ReplyObjectMeta
	ObjectMetaScan(path, cursor, end string, limit uint64) *ReplyObjectMetaList
}

func _obj_clean_path(path string) string {
	return strings.Trim(filepath.Clean(path), "/")
}

func _obj_str_hash(str string, num int) []byte {

	if num < 1 {
		num = 1
	} else if num > 32 {
		num = 32
	}

	h := sha1.New()
	io.WriteString(h, str)

	return h.Sum(nil)[:num]
}

func ObjectPathSplit(path string) (string, string, string, []byte, []byte) {

	path = _obj_clean_path(path)

	fold, field := "", path

	if i := strings.LastIndex(path, "/"); i > 0 {
		fold, field = path[:i], path[i+1:]
	}

	return path, fold, field,
		_obj_str_hash(fold, obj_fold_len), _obj_str_hash(field, obj_field_len)
}

func _obj_entry_key_prefix(key []byte) []byte {

	si := len(key)
	if si > 255 {
		si = 255
	}

	return append([]byte{NsObjectEntry, uint8(si)}, key[:si]...)
}

func _obj_entry_meta_prefix(key []byte) []byte {
	return append([]byte{ns_object_meta, uint8(len(key))}, key...)
}

func ObjectRandomKey(length int) []byte {

	if length < 1 {
		length = 1
	} else if length > 16 {
		length = 16
	}

	key := make([]byte, length)

	io.ReadFull(rand.Reader, key)

	return key
}

func ObjectStringHex(key string) []byte {

	if v, err := hex.DecodeString(key); err == nil {
		return v
	}

	return []byte{}
}

func ObjectHexString(key []byte) string {
	return hex.EncodeToString(key)
}

func ObjectEntryIndex(path string) []byte {

	_, _, _, fold, field := ObjectPathSplit(path)

	return append(_obj_entry_key_prefix(fold), field...)
}

func ObjectEntryFold(path string) []byte {
	return _obj_entry_key_prefix(_obj_str_hash(_obj_clean_path(path), obj_fold_len))
}

func ObjectMetaIndex(path string) []byte {

	_, _, _, fold, field := ObjectPathSplit(path)

	return append(_obj_entry_meta_prefix(fold), field...)
}

func ObjectMetaFold(path string) []byte {
	return _obj_entry_meta_prefix(_obj_str_hash(_obj_clean_path(path), obj_fold_len))
}

type ReplyObjectMeta struct {
	*Reply   `json:",omitempty"`
	Type     uint8  `json:"type,omitempty"`     // 1
	Len      uint32 `json:"len,omitempty"`      // 4
	Size     uint64 `json:"size,omitempty"`     // 8
	Version  uint64 `json:"version,omitempty"`  // 8
	Created  uint64 `json:"created,omitempty"`  // 8
	Updated  uint64 `json:"updated,omitempty"`  // 8
	Ttl      uint32 `json:"ttl,omitempty"`      // 4
	Group    uint32 `json:"group,omitempty"`    // 4
	User     uint32 `json:"user,omitempty"`     // 4
	Mode     uint8  `json:"mode,omitempty"`     // 1
	SumCheck uint32 `json:"sumcheck,omitempty"` // 4
	NameLen  uint16 `json:"namelen,omitempty"`  // 2
	Name     string `json:"name,omitempty"`     // n
	data     []byte `json:",omitempty"`
}

type ReplyObjectMetaList struct {
	*Reply `json:",omitempty"`
	Items  []ReplyObjectMeta `json:"items,omitempty"`
}

func ObjectMetaParse(data []byte) ReplyObjectMeta {

	for i := len(data); i < 56; i++ {
		data = append(data, 0x00)
	}

	return ReplyObjectMeta{
		//
		Type: data[0],
		//
		Len:  binary.BigEndian.Uint32(data[1:5]),
		Size: binary.BigEndian.Uint64(data[5:13]),
		//
		Version: binary.BigEndian.Uint64(data[13:21]),
		Created: binary.BigEndian.Uint64(data[21:29]),
		Updated: binary.BigEndian.Uint64(data[29:37]),
		//
		Ttl:   binary.BigEndian.Uint32(data[37:41]),
		Group: binary.BigEndian.Uint32(data[41:45]),
		User:  binary.BigEndian.Uint32(data[45:49]),
		Mode:  data[49],
		//
		SumCheck: binary.BigEndian.Uint32(data[50:54]),
		//
		NameLen: binary.BigEndian.Uint16(data[54:56]),
		Name:    string(data[56:]),
		//
		data: data,
	}
}

func (m *ReplyObjectMeta) Export() []byte {

	for i := len(m.data); i < 56; i++ {
		m.data = append(m.data, 0x00)
	}

	m.data[0] = m.Type

	//
	binary.BigEndian.PutUint32(m.data[1:5], m.Len)
	binary.BigEndian.PutUint64(m.data[5:13], m.Size)

	//
	binary.BigEndian.PutUint64(m.data[13:21], m.Version)
	binary.BigEndian.PutUint64(m.data[21:29], m.Created)
	binary.BigEndian.PutUint64(m.data[29:37], m.Updated)

	//
	binary.BigEndian.PutUint32(m.data[37:41], m.Ttl)
	binary.BigEndian.PutUint32(m.data[41:45], m.Group)
	binary.BigEndian.PutUint32(m.data[45:49], m.User)

	//
	binary.BigEndian.PutUint16(m.data[54:56], m.NameLen)
	m.data = append(m.data[:56], []byte(m.Name)...)

	return m.data
}

// func (om *ReplyObjectMeta) MarshalBinary() (data []byte, err error) {
// 	return []byte{}, nil
// }

// func (om *ReplyObjectMeta) UnmarshalBinary(data []byte) error {
// 	return nil
// }
