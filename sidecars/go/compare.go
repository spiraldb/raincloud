// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// Compare batches independently of their boundaries. Only one batch from each reader is live.
func compareRecords(got pqarrow.RecordReader, expected *ipc.FileReader) error {
	if got.Schema().NumFields() != expected.Schema().NumFields() {
		return fmt.Errorf("column count differs")
	}
	for i, f := range expected.Schema().Fields() {
		if got.Schema().Field(i).Name != f.Name {
			return fmt.Errorf("column names differ")
		}
		if !compatibleTypes(got.Schema().Field(i).Type, f.Type) {
			return fmt.Errorf("column %q has incompatible types", f.Name)
		}
	}
	var actual arrow.RecordBatch
	var offset int64
	next := func() bool {
		for got.Next() {
			actual = got.RecordBatch()
			offset = 0
			if actual.NumRows() != 0 {
				return true
			}
		}
		actual = nil
		return false
	}
	for i := 0; i < expected.NumRecords(); i++ {
		batch, err := expected.RecordBatchAt(i)
		if err != nil {
			return err
		}
		err = func() error {
			defer batch.Release()
			for start := int64(0); start < batch.NumRows(); {
				if actual == nil || offset == actual.NumRows() {
					if !next() {
						if got.Err() != nil {
							return got.Err()
						}
						return fmt.Errorf("row count differs")
					}
				}
				n := min(batch.NumRows()-start, actual.NumRows()-offset)
				for c := 0; c < int(batch.NumCols()); c++ {
					a := array.NewSlice(actual.Column(c), offset, offset+n)
					b := array.NewSlice(batch.Column(c), start, start+n)
					equal := logicalEqual(a, b)
					a.Release()
					b.Release()
					if !equal {
						return fmt.Errorf("data mismatch in column %q", batch.ColumnName(c))
					}
				}
				start += n
				offset += n
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	if actual != nil && offset < actual.NumRows() || next() {
		return fmt.Errorf("row count differs")
	}
	return got.Err()
}

// A safe cast alone permits lossy float narrowing and int->bool. Require the
// reverse cast to recover the original values, including float sign bits.
func logicalEqual(a, b arrow.Array) bool {
	if !compatibleTypes(a.DataType(), b.DataType()) {
		return false
	}
	if ext, ok := a.(array.ExtensionArray); ok {
		a = ext.Storage()
	}
	if ext, ok := b.(array.ExtensionArray); ok {
		b = ext.Storage()
	}
	if a.Len() != b.Len() {
		return false
	}
	for i := 0; i < a.Len(); i++ {
		if logicalNull(a, i) != logicalNull(b, i) {
			return false
		}
	}
	// Compare dictionary values, not their index representation. Go has no
	// general cast from primitive values back into a dictionary.
	_, adict := a.(*array.Dictionary)
	_, bdict := b.(*array.Dictionary)
	_, aunion := a.(array.Union)
	_, bunion := b.(array.Union)
	if adict || bdict || aunion || bunion {
		for i := 0; i < a.Len(); i++ {
			if logicalNull(a, i) {
				continue
			}
			x, y := valueSlice(a, i), valueSlice(b, i)
			equal := logicalEqual(x, y)
			x.Release()
			y.Release()
			if !equal {
				return false
			}
		}
		return true
	}
	// Nested and binary representations normalize without casting whole
	// arrays. This also ignores arbitrary payload beneath null parents.
	switch a.(type) {
	case *array.Struct, listArray:
		return exactEqual(a, b)
	case interface{ Value(int) []byte }:
		left := a.(interface{ Value(int) []byte })
		right, ok := b.(interface{ Value(int) []byte })
		if !ok {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) && !bytes.Equal(left.Value(i), right.Value(i)) {
				return false
			}
		}
		return true
	}
	if arrow.TypeEqual(a.DataType(), b.DataType()) {
		return exactEqual(a, b)
	}
	cast, err := compute.CastArray(context.Background(), a, compute.SafeCastOptions(b.DataType()))
	if err != nil {
		return false
	}
	defer cast.Release()
	back, err := compute.CastArray(context.Background(), cast, compute.SafeCastOptions(a.DataType()))
	if err != nil {
		return false
	}
	defer back.Release()
	return exactEqual(a, back) && exactEqual(cast, b)
}

func valueSlice(a arrow.Array, i int) arrow.Array {
	if dict, ok := a.(*array.Dictionary); ok {
		index := int64(dict.GetValueIndex(i))
		return array.NewSlice(dict.Dictionary(), index, index+1)
	}
	if union, ok := a.(array.Union); ok {
		index := int64(i)
		if dense, ok := a.(*array.DenseUnion); ok {
			index = int64(dense.ValueOffset(i))
		}
		return array.NewSlice(union.Field(union.ChildID(i)), index, index+1)
	}
	return array.NewSlice(a, int64(i), int64(i+1))
}

func logicalNull(a arrow.Array, i int) bool {
	if a.DataType().ID() == arrow.NULL || a.IsNull(i) {
		return true
	}
	if ext, ok := a.(array.ExtensionArray); ok {
		return logicalNull(ext.Storage(), i)
	}
	if dict, ok := a.(*array.Dictionary); ok {
		return logicalNull(dict.Dictionary(), dict.GetValueIndex(i))
	}
	if union, ok := a.(array.Union); ok {
		index := i
		if dense, ok := a.(*array.DenseUnion); ok {
			index = int(dense.ValueOffset(i))
		}
		return logicalNull(union.Field(union.ChildID(i)), index)
	}
	return false
}

type listArray interface {
	arrow.Array
	ValueOffsets(int) (int64, int64)
	ListValues() arrow.Array
}

// Ignore payload under null parents; preserve signed zero and NaN payloads at
// every nesting level. Arrow's approximate comparator collapses signed zero.
func exactEqual(a, b arrow.Array) bool {
	if a.Len() != b.Len() || a.NullN() != b.NullN() {
		return false
	}
	switch left := a.(type) {
	case *array.Float16:
		right, ok := b.(*array.Float16)
		if !ok {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) != b.IsNull(i) {
				return false
			}
			if a.IsNull(i) {
				continue
			}
			x, y := left.Value(i), right.Value(i)
			if x.Uint16() != y.Uint16() {
				return false
			}
		}
		return true
	case *array.Float64:
		right, ok := b.(*array.Float64)
		if !ok {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) != b.IsNull(i) {
				return false
			}
			if a.IsNull(i) {
				continue
			}
			x, y := left.Value(i), right.Value(i)
			if math.Float64bits(x) != math.Float64bits(y) {
				return false
			}
		}
		return true
	case *array.Float32:
		right, ok := b.(*array.Float32)
		if !ok {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) != b.IsNull(i) {
				return false
			}
			if a.IsNull(i) {
				continue
			}
			x, y := left.Value(i), right.Value(i)
			if math.Float32bits(x) != math.Float32bits(y) {
				return false
			}
		}
		return true
	case *array.Struct:
		right, ok := b.(*array.Struct)
		if !ok || left.NumField() != right.NumField() {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) != b.IsNull(i) {
				return false
			}
			if a.IsNull(i) {
				continue
			}
			for c := 0; c < left.NumField(); c++ {
				x, y := array.NewSlice(left.Field(c), int64(i), int64(i+1)), array.NewSlice(right.Field(c), int64(i), int64(i+1))
				equal := logicalEqual(x, y)
				x.Release()
				y.Release()
				if !equal {
					return false
				}
			}
		}
		return true
	case listArray:
		right, ok := b.(listArray)
		if !ok {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) != b.IsNull(i) {
				return false
			}
			if a.IsNull(i) {
				continue
			}
			lo, hi := left.ValueOffsets(i)
			ro, rh := right.ValueOffsets(i)
			x, y := array.NewSlice(left.ListValues(), lo, hi), array.NewSlice(right.ListValues(), ro, rh)
			equal := logicalEqual(x, y)
			x.Release()
			y.Release()
			if !equal {
				return false
			}
		}
		return true
	case *array.Dictionary:
		right, ok := b.(*array.Dictionary)
		if !ok {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) != b.IsNull(i) {
				return false
			}
			if a.IsNull(i) {
				continue
			}
			li, ri := int64(left.GetValueIndex(i)), int64(right.GetValueIndex(i))
			x, y := array.NewSlice(left.Dictionary(), li, li+1), array.NewSlice(right.Dictionary(), ri, ri+1)
			equal := logicalEqual(x, y)
			x.Release()
			y.Release()
			if !equal {
				return false
			}
		}
		return true
	default:
		return array.Equal(a, b)
	}
}

// Match the logical families in export/compare.py independently of values.
// Empty arrays and null containers must satisfy the same schema contract.
func compatibleTypes(a, b arrow.DataType) bool {
	if ext, ok := a.(arrow.ExtensionType); ok {
		a = ext.StorageType()
	}
	if ext, ok := b.(arrow.ExtensionType); ok {
		b = ext.StorageType()
	}
	if member := nullableMember(a); member != nil {
		return compatibleTypes(member, b)
	}
	if member := nullableMember(b); member != nil {
		return compatibleTypes(a, member)
	}
	if arrow.TypeEqual(a, b) {
		return true
	}
	if d, ok := a.(*arrow.DictionaryType); ok {
		return compatibleTypes(d.ValueType, b)
	}
	if d, ok := b.(*arrow.DictionaryType); ok {
		return compatibleTypes(a, d.ValueType)
	}
	if left, ok := a.(*arrow.StructType); ok {
		right, ok := b.(*arrow.StructType)
		return ok && compatibleFields(left.Fields(), right.Fields())
	}
	if left, ok := a.(*arrow.MapType); ok {
		right, ok := b.(*arrow.MapType)
		return ok && compatibleFields([]arrow.Field{left.KeyField(), left.ItemField()}, []arrow.Field{right.KeyField(), right.ItemField()})
	}
	lists := func(t arrow.Type) bool {
		return t == arrow.LIST || t == arrow.LARGE_LIST || t == arrow.FIXED_SIZE_LIST || t == arrow.LIST_VIEW || t == arrow.LARGE_LIST_VIEW
	}
	if lists(a.ID()) && lists(b.ID()) {
		if left, ok := a.(*arrow.FixedSizeListType); ok {
			if right, ok := b.(*arrow.FixedSizeListType); ok && left.Len() != right.Len() {
				return false
			}
		}
		return compatibleTypes(a.(interface{ Elem() arrow.DataType }).Elem(), b.(interface{ Elem() arrow.DataType }).Elem())
	}
	if left, ok := a.(*arrow.TimestampType); ok {
		right, ok := b.(*arrow.TimestampType)
		return ok && (left.TimeZone == "") == (right.TimeZone == "")
	}
	if left, ok := a.(arrow.DecimalType); ok && arrow.IsInteger(b.ID()) {
		return left.GetScale() == 0
	}
	if right, ok := b.(arrow.DecimalType); ok && arrow.IsInteger(a.ID()) {
		return right.GetScale() == 0
	}
	if left, ok := a.(*arrow.FixedSizeBinaryType); ok {
		if right, ok := b.(*arrow.FixedSizeBinaryType); ok && left.ByteWidth != right.ByteWidth {
			return false
		}
	}
	strings := func(t arrow.Type) bool { return t == arrow.STRING || t == arrow.LARGE_STRING || t == arrow.STRING_VIEW }
	binary := func(t arrow.Type) bool {
		return t == arrow.BINARY || t == arrow.LARGE_BINARY || t == arrow.BINARY_VIEW || t == arrow.FIXED_SIZE_BINARY
	}
	date := func(t arrow.Type) bool { return t == arrow.DATE32 || t == arrow.DATE64 }
	time := func(t arrow.Type) bool { return t == arrow.TIME32 || t == arrow.TIME64 }
	for _, family := range []func(arrow.Type) bool{arrow.IsInteger, arrow.IsFloating, arrow.IsDecimal, strings, binary, date, time} {
		if family(a.ID()) && family(b.ID()) {
			return true
		}
	}
	return a.ID() == arrow.DURATION && b.ID() == arrow.DURATION
}

func nullableMember(t arrow.DataType) arrow.DataType {
	if union, ok := t.(arrow.UnionType); ok {
		fields := union.Fields()
		if len(fields) == 2 {
			if fields[0].Type.ID() == arrow.NULL {
				return fields[1].Type
			}
			if fields[1].Type.ID() == arrow.NULL {
				return fields[0].Type
			}
		}
	}
	return nil
}

func compatibleFields(a, b []arrow.Field) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || !compatibleTypes(a[i].Type, b[i].Type) {
			return false
		}
	}
	return true
}
