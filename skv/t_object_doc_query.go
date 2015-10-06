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
	"strings"
)

const (
	ObjectDocQueryFilterValues   uint8 = 0
	ObjectDocQueryFilterRange    uint8 = 1
	ObjectDocQueryOffsetMax            = 100000
	ObjectDocQueryLimitMax             = 10000
	_object_doc_query_filter_max       = 4
)

const (
	ObjectDocQuerySortAttrAsc  = "asc"
	ObjectDocQuerySortAttrDesc = "desc"
)

type ObjectDocQuerySet struct {
	Filters   []ObjectDocQueryFilter
	Limit     int
	Offset    int
	SortField string
	SortMode  string
}

type ObjectDocQueryFilter struct {
	Type    uint8
	Field   string
	Values  []string
	Exclude bool
}

func NewObjectDocQuery() *ObjectDocQuerySet {

	return &ObjectDocQuerySet{
		Limit:   1,
		Filters: []ObjectDocQueryFilter{},
	}
}

func (q *ObjectDocQuerySet) Filter(field string, values []string, exclude bool) *ObjectDocQuerySet {

	if len(q.Filters) < _object_doc_query_filter_max {

		q.Filters = append(q.Filters, ObjectDocQueryFilter{
			Type:    ObjectDocQueryFilterValues,
			Field:   field,
			Values:  values,
			Exclude: exclude,
		})
	}

	return q
}

func (q *ObjectDocQuerySet) Limits(offset, limit int) *ObjectDocQuerySet {

	if offset > ObjectDocQueryOffsetMax {
		q.Offset = ObjectDocQueryOffsetMax
	} else if offset < 0 {
		q.Offset = 0
	} else {
		q.Offset = offset
	}

	if limit > ObjectDocQueryLimitMax {
		q.Limit = ObjectDocQueryLimitMax
	} else if limit < 1 {
		q.Limit = 1
	} else {
		q.Limit = limit
	}

	return q
}

func (q *ObjectDocQuerySet) Sort(mode, field string) *ObjectDocQuerySet {

	q.SortMode = strings.ToLower(mode)
	if q.SortMode != ObjectDocQuerySortAttrDesc {
		q.SortMode = ObjectDocQuerySortAttrAsc
	}

	q.SortField = strings.ToLower(field)

	return q
}
