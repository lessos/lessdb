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
	"time"
)

const (
	ObjectTypeFold     = 0x01
	ObjectTypeGeneral  = 0x0a
	ObjectTypeDocument = 0x0b

	obj_meta_ver = 0x01
)

// meta bytes define
//
//  - meta version 1  0:1
//  - meta type    1  1:2
//  - meta seek    2  2:4
//
//  - log version  8  4:12
//  - version      8 12:20
//  - created      8 20:28
//  - updated      8 28:36
//  - expired      8 36:44
//
//  - size         8 44:52
//  - data sum     4 52:56
//  - fold num     4 52:56
//
//  - group        4 56:60
//  - user         4 60:64
//  - mode         4 64:68
//
// params
//  - name len     1 68:69
//  - name data    n
//
type ObjectMeta struct {
	seek       int
	LogVersion uint64 `json:"log_version,omitempty"`
	Type       uint8  `json:"type"`
	Version    uint64 `json:"version,omitempty"`
	Created    uint64 `json:"created"`
	Updated    uint64 `json:"updated"`
	Expired    uint64 `json:"expired,omitempty"`
	Size       uint64 `json:"size"`
	Num        uint32 `json:"num,omitempty"`
	Sum        uint32 `json:"sum,omitempty"`
	Group      uint32 `json:"group,omitempty"`
	User       uint32 `json:"user,omitempty"`
	Mode       uint32 `json:"mode,omitempty"`
	Name       string `json:"name"`
	Ttl        int64  `json:"ttl,omitempty"`
}

func (m *ObjectMeta) Export() []byte {

	data := make([]byte, 69)

	//
	data[0] = obj_meta_ver
	data[1] = m.Type

	//
	binary.BigEndian.PutUint64(data[12:20], m.Version)
	binary.BigEndian.PutUint64(data[4:12], m.LogVersion)

	//
	if m.Expired == 0 && m.Ttl > 0 {
		m.Expired = MetaTimeNowAddMS(m.Ttl)
	}

	binary.BigEndian.PutUint64(data[20:28], m.Created)
	binary.BigEndian.PutUint64(data[28:36], m.Updated)
	binary.BigEndian.PutUint64(data[36:44], m.Expired)

	//
	binary.BigEndian.PutUint32(data[56:60], m.Group)
	binary.BigEndian.PutUint32(data[60:64], m.User)
	binary.BigEndian.PutUint32(data[64:68], m.Mode)

	//
	if m.Type == ObjectTypeFold {
		binary.BigEndian.PutUint32(data[52:56], m.Num)
	} else if m.Type == ObjectTypeGeneral {
		binary.BigEndian.PutUint32(data[52:56], m.Sum)
	}

	//
	binary.BigEndian.PutUint64(data[44:52], m.Size)

	//
	namelen := len(m.Name)
	if namelen > 255 {
		namelen = 255
	}

	if namelen > 0 {
		data[68] = uint8(namelen)
		data = append(data, []byte(m.Name[:namelen])...)
	}

	binary.BigEndian.PutUint16(data[2:4], uint16(len(data)))

	return data
}

func (m *ObjectMeta) SeekLength() int {
	return m.seek
}

func ObjectMetaParse(data []byte) ObjectMeta {

	m := ObjectMeta{}

	if len(data) > 69 {

		//
		m.Type = data[1]
		m.seek = int(binary.BigEndian.Uint16(data[2:4]))

		//
		m.LogVersion = binary.BigEndian.Uint64(data[4:12])
		m.Version = binary.BigEndian.Uint64(data[12:20])

		//
		m.Created = binary.BigEndian.Uint64(data[20:28])
		m.Updated = binary.BigEndian.Uint64(data[28:36])
		m.Expired = binary.BigEndian.Uint64(data[36:44])

		//
		m.Group = binary.BigEndian.Uint32(data[56:60])
		m.User = binary.BigEndian.Uint32(data[60:64])
		m.Mode = binary.BigEndian.Uint32(data[64:68])

		//
		if m.Type == ObjectTypeFold {
			m.Num = binary.BigEndian.Uint32(data[52:56])
		} else if m.Type == ObjectTypeGeneral {
			m.Sum = binary.BigEndian.Uint32(data[52:56])
		}

		//
		m.Size = binary.BigEndian.Uint64(data[44:52])

		//
		m.Ttl = (MetaTimeParse(m.Expired).UnixNano() - time.Now().UTC().UnixNano()) / 1e6

		//
		if len(data) >= m.seek {
			m.Name = string(data[69:m.seek])
		}
	}

	return m
}
