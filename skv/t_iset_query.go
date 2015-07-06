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
	QueryFilterValues uint8 = 0
	QueryFilterRange  uint8 = 1
	QueryFilterMax          = 4
	QueryOffsetMax          = 100000
	QueryLimitMax           = 10000
)

const (
	QuerySortAttrAsc  = "asc"
	QuerySortAttrDesc = "desc"
)

type QuerySet struct {
	Filters   []QueryFilter
	limit     int
	offset    int
	sortField string
	sortMode  string
}

type QueryFilter struct {
	Type    uint8
	Field   string
	Values  []string
	Exclude bool
}

func NewQuery() *QuerySet {
	return &QuerySet{
		limit:   1,
		Filters: []QueryFilter{},
	}
}

func (q *QuerySet) Filter(field string, values []string, exclude bool) *QuerySet {

	if len(q.Filters) < QueryFilterMax {

		q.Filters = append(q.Filters, QueryFilter{
			Type:    QueryFilterValues,
			Field:   field,
			Values:  values,
			Exclude: exclude,
		})
	}

	return q
}

// func (q *QuerySet) FilterRange(fit string, min, max uint64, exclude bool) *QuerySet {
// 	return q
// }

func (q *QuerySet) Limits(offset, limit int) *QuerySet {

	if offset > QueryOffsetMax {
		q.offset = QueryOffsetMax
	} else if offset < 0 {
		q.offset = 0
	} else {
		q.offset = offset
	}

	if limit > QueryLimitMax {
		q.limit = QueryLimitMax
	} else if limit < 1 {
		q.limit = 1
	} else {
		q.limit = limit
	}

	return q
}

func (q *QuerySet) Sort(mode, field string) *QuerySet {

	q.sortMode = strings.ToLower(mode)
	if q.sortMode != QuerySortAttrDesc {
		q.sortMode = QuerySortAttrAsc
	}

	q.sortField = strings.ToLower(field)

	return q
}
