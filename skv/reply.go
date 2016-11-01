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
	"errors"

	"github.com/lessos/lessdb/dbtypes"
)

const (
	ReplyOK          = "OK"
	ReplyNotFound    = "NotFound"
	ReplyBadArgument = "BadArgument"
)

type Reply struct {
	Status string
	Data   []dbtypes.Bytex
}

func NewReply(status string) *Reply {

	if status == "" {
		status = ReplyOK
	}

	return &Reply{
		Status: status,
	}
}

func (r *Reply) bytex() *dbtypes.Bytex {

	if len(r.Data) > 0 {
		return &r.Data[0]
	}

	return &dbtypes.Bytex{}
}

func (r *Reply) Bytes() []byte {
	return r.bytex().Bytes()
}

func (r *Reply) String() string {
	return r.bytex().String()
}

func (r *Reply) Int() int {
	return r.bytex().Int()
}

func (r *Reply) Int8() int8 {
	return r.bytex().Int8()
}

func (r *Reply) Int16() int16 {
	return r.bytex().Int16()
}

func (r *Reply) Int32() int32 {
	return r.bytex().Int32()
}

func (r *Reply) Int64() int64 {
	return r.bytex().Int64()
}

func (r *Reply) Uint() uint {
	return r.bytex().Uint()
}

func (r *Reply) Uint8() uint8 {
	return r.bytex().Uint8()
}

func (r *Reply) Uint16() uint16 {
	return r.bytex().Uint16()
}

func (r *Reply) Uint32() uint32 {
	return r.bytex().Uint32()
}

func (r *Reply) Uint64() uint64 {
	return r.bytex().Uint64()
}

func (r *Reply) Float32() float32 {
	return r.bytex().Float32()
}

func (r *Reply) Float64() float64 {
	return r.bytex().Float64()
}

func (r *Reply) Bool() bool {
	return r.bytex().Bool()
}

func (r *Reply) List() []dbtypes.Bytex {
	return r.Data
}

func (r *Reply) Hash() []ReplyEntry {

	ls := []ReplyEntry{}

	for i := 0; i < (len(r.Data) - 1); i += 2 {
		ls = append(ls, ReplyEntry{r.Data[i], r.Data[i+1]})
	}

	return ls
}

func (r *Reply) KvLen() int {
	return len(r.Data) / 2
}

func (r *Reply) KvEach(fn func(key, value dbtypes.Bytex)) int {

	if len(r.Data) < 2 {
		return 0
	}

	for i := 0; i < (len(r.Data) - 1); i += 2 {
		fn(r.Data[i], r.Data[i+1])
	}

	return r.KvLen()
}

// Json returns the map that marshals from the reply bytes as json in response .
func (r *Reply) JsonDecode(v interface{}) error {

	if len(r.Data) < 1 {
		return errors.New("json: invalid format")
	}

	return r.Data[0].JsonDecode(&v)
}

func (r *Reply) Object() *Object {

	o := &Object{
		Status: r.Status,
		Meta:   ObjectMetaParse(r.Bytes()),
	}

	if o.Meta.seek > 0 {
		o.Data = r.Bytes()[o.Meta.seek:]
	}

	return o
}

func (r *Reply) ObjectList() []*Object {

	ls := []*Object{}

	for i := 0; i < (len(r.Data) - 1); i += 2 {

		o := &Object{
			Meta: ObjectMetaParse(r.Data[i+1]),
			Key:  r.Data[i],
		}

		if o.Meta.seek > 0 {
			o.Data = r.Data[i+1][o.Meta.seek:]
		}

		ls = append(ls, o)
	}

	return ls
}

func (r *Reply) ObjectMeta() ObjectMeta {
	return ObjectMetaParse(r.Bytes())
}

func (r *Reply) ObjectMetaList() []ObjectMeta {

	ls := []ObjectMeta{}

	for i := 0; i < (len(r.Data) - 1); i += 2 {
		ls = append(ls, ObjectMetaParse(r.Data[i+1]))
	}

	return ls
}

type ReplyEntry struct {
	Key, Value dbtypes.Bytex
}

func (e *ReplyEntry) String() string {
	return e.Value.String()
}

func (e *ReplyEntry) Uint64() uint64 {
	return e.Value.Uint64()
}

func (e *ReplyEntry) JsonDecode(v interface{}) error {
	return e.Value.JsonDecode(&v)
}
