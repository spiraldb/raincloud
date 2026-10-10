// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package main

import (
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
	}
	var actual arrow.RecordBatch
	var offset, rows int64
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
		rows += batch.NumRows()
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
	if rows == 0 {
		for i, f := range expected.Schema().Fields() {
			if !emptyTypesEqual(got.Schema().Field(i).Type, f.Type) {
				return fmt.Errorf("empty column %q has incompatible types", f.Name)
			}
		}
	}
	return got.Err()
}

// A safe cast alone permits lossy float narrowing and int->bool. Require the
// reverse cast to recover the original values, including float sign bits.
func logicalEqual(a, b arrow.Array) bool {
	if ext, ok := a.(array.ExtensionArray); ok {
		a = ext.Storage()
	}
	if ext, ok := b.(array.ExtensionArray); ok {
		b = ext.Storage()
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

type listArray interface {
	arrow.Array
	ValueOffsets(int) (int64, int64)
	ListValues() arrow.Array
}

// Ignore payload under null parents; preserve signed zero and accept NaNs at
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
			if x.Uint16() != y.Uint16() && !(math.IsNaN(float64(x.Float32())) && math.IsNaN(float64(y.Float32()))) {
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
			if math.Float64bits(x) != math.Float64bits(y) && !(math.IsNaN(x) && math.IsNaN(y)) {
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
			if math.Float32bits(x) != math.Float32bits(y) && !(math.IsNaN(float64(x)) && math.IsNaN(float64(y))) {
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

// Empty files still have a schema. Reversible casts of empty arrays alone
// would incorrectly accept unrelated types such as string and integer.
func emptyTypesEqual(a, b arrow.DataType) bool {
	if ext, ok := a.(arrow.ExtensionType); ok {
		a = ext.StorageType()
	}
	if ext, ok := b.(arrow.ExtensionType); ok {
		b = ext.StorageType()
	}
	if arrow.TypeEqual(a, b) {
		return true
	}
	if d, ok := a.(*arrow.DictionaryType); ok {
		return emptyTypesEqual(d.ValueType, b)
	}
	if d, ok := b.(*arrow.DictionaryType); ok {
		return emptyTypesEqual(a, d.ValueType)
	}
	strings := func(t arrow.Type) bool { return t == arrow.STRING || t == arrow.LARGE_STRING || t == arrow.STRING_VIEW }
	binary := func(t arrow.Type) bool { return t == arrow.BINARY || t == arrow.LARGE_BINARY || t == arrow.BINARY_VIEW }
	return strings(a.ID()) && strings(b.ID()) || binary(a.ID()) && binary(b.ID())
}
