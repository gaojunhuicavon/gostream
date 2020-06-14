package gostream

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	testSerialErrFloat64Stream   = &errFloat64Stream{err: errors.New(""), parallel: false}
	testParallelErrFloat64Stream = &errFloat64Stream{err: errors.New(""), parallel: true}
)

func Test_errFloat64Stream_Average(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		ans, err := testSerialErrFloat64Stream.Average()
		assert.Nil(t, ans)
		assert.Error(t, err)
	})
	t.Run("test parallel", func(t *testing.T) {
		ans, err := testParallelErrFloat64Stream.Average()
		assert.Nil(t, ans)
		assert.Error(t, err)
	})
}

func Test_errFloat64Stream_Collect(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		ans, err := testSerialErrFloat64Stream.Collect()
		assert.Nil(t, ans)
		assert.Error(t, err)
	})
	t.Run("test parallel", func(t *testing.T) {
		ans, err := testParallelErrFloat64Stream.Collect()
		assert.Nil(t, ans)
		assert.Error(t, err)
	})
}

func Test_errFloat64Stream_Distinct(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		s := testSerialErrFloat64Stream.Distinct()
		assert.False(t, s.IsParallel())
		ans, err := s.Collect()
		assert.Nil(t, ans)
		assert.Error(t, err)
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrFloat64Stream.Distinct()
		assert.True(t, s.IsParallel())
		ans, err := s.Collect()
		assert.Nil(t, ans)
		assert.Error(t, err)
	})
}

func Test_errFloat64Stream_Filter(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		s := testSerialErrFloat64Stream.Filter(func(val float64) (match bool) {
			return false
		})
		assert.Error(t, s.Err())
		assert.False(t, s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrFloat64Stream.Filter(func(val float64) (match bool) {
			return false
		})
		assert.Error(t, s.Err())
		assert.True(t, s.IsParallel())
	})
}

func Test_errFloat64Stream_FlatMap(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		s := testSerialErrFloat64Stream.FlatMap(func(val float64) Float64Stream {
			return &sequentialFloat64Stream{elements: []float64{1, 2, 3}}
		})
		assert.Error(t, s.Err())
		assert.False(t, s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrFloat64Stream.FlatMap(func(val float64) Float64Stream {
			return &sequentialFloat64Stream{elements: []float64{1, 2, 3}}
		})
		assert.Error(t, s.Err())
		assert.True(t, s.IsParallel())
	})
}

func Test_errFloat64Stream_Limit(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		s := testSerialErrFloat64Stream.Limit(1)
		assert.Error(t, s.Err())
		assert.False(t, s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrFloat64Stream.Limit(1)
		assert.Error(t, s.Err())
		assert.True(t, s.IsParallel())
	})
}

func Test_errFloat64Stream_Map(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		s := testSerialErrFloat64Stream.Map(func(src float64) (dest float64) {
			return src
		})
		assert.Error(t, s.Err())
		assert.False(t, s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrFloat64Stream.Map(func(src float64) (dest float64) {
			return src
		})
		assert.Error(t, s.Err())
		assert.True(t, s.IsParallel())
	})
}

func Test_errFloat64Stream_MapToInt(t *testing.T) {
	s := testSerialErrFloat64Stream.MapToInt(func(src float64) (dest int) {
		return int(src)
	})
	assert.Error(t, s.Err())
}

func Test_errFloat64Stream_MapToObj(t *testing.T) {
	at := assert.New(t)
	t.Run("test serial", func(t *testing.T) {
		s := testSerialErrFloat64Stream.MapToObj(func(src float64) (dest interface{}) {
			return &struct{}{}
		})
		at.Error(s.Err())
		at.False(s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrFloat64Stream.MapToObj(func(src float64) (dest interface{}) {
			return &struct{}{}
		})
		at.Error(s.Err())
		at.True(s.IsParallel())
	})
}

func Test_errFloat64Stream_Max(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		res, err := testSerialErrFloat64Stream.Max()
		assert.Error(t, err)
		assert.Nil(t, res)
	})
	t.Run("test parallel", func(t *testing.T) {
		res, err := testParallelErrFloat64Stream.Max()
		assert.Error(t, err)
		assert.Nil(t, res)
	})
}

func Test_errFloat64Stream_Min(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		res, err := testSerialErrFloat64Stream.Min()
		assert.Error(t, err)
		assert.Nil(t, res)
	})
	t.Run("test parallel", func(t *testing.T) {
		res, err := testParallelErrFloat64Stream.Min()
		assert.Error(t, err)
		assert.Nil(t, res)
	})
}

func Test_errFloat64Stream_Parallel(t *testing.T) {
	at := assert.New(t)
	t.Run("test sequential to parallel", func(t *testing.T) {
		dest := testSerialErrFloat64Stream.Parallel()
		at.True(dest.IsParallel())
		at.Error(dest.Err())
	})

	t.Run("test parallel to parallel", func(t *testing.T) {
		dest := testParallelErrFloat64Stream.Parallel()
		at.True(dest.IsParallel())
		at.Error(dest.Err())
	})
}

func Test_errFloat64Stream_Reduce(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		at := assert.New(t)
		res, err := testSerialErrFloat64Stream.Reduce(func(a, b float64) (c float64) {
			return a + b
		})
		at.Error(err)
		at.Nil(res)
	})
	t.Run("test parallel", func(t *testing.T) {
		at := assert.New(t)
		res, err := testParallelErrFloat64Stream.Reduce(func(a, b float64) (c float64) {
			return a + b
		})
		at.Error(err)
		at.Nil(res)
	})
}

func Test_errFloat64Stream_Sequential(t *testing.T) {
	at := assert.New(t)
	t.Run("to sequential to sequential", func(t *testing.T) {
		dest := testSerialErrFloat64Stream.Sequential()
		at.False(dest.IsParallel())
		at.Error(dest.Err())
	})
	t.Run("to parallel to sequential", func(t *testing.T) {
		dest := testParallelErrFloat64Stream.Sequential()
		at.False(dest.IsParallel())
		at.Error(dest.Err())
	})
}

func Test_errFloat64Stream_Skip(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		s := testSerialErrFloat64Stream.Skip(1)
		assert.Error(t, s.Err())
		assert.False(t, s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrFloat64Stream.Skip(1)
		assert.Error(t, s.Err())
		assert.True(t, s.IsParallel())
	})
}

func Test_errFloat64Stream_Sorted(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		s := testSerialErrFloat64Stream.Sorted()
		assert.Error(t, s.Err())
		assert.False(t, s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrFloat64Stream.Sorted()
		assert.Error(t, s.Err())
		assert.True(t, s.IsParallel())
	})
}

func Test_errFloat64Stream_Sum(t *testing.T) {
	t.Run("test serial", func(t *testing.T) {
		sum, err := testSerialErrFloat64Stream.Sum()
		assert.Error(t, err)
		assert.Zero(t, sum)
	})
	t.Run("test parallel", func(t *testing.T) {
		sum, err := testParallelErrFloat64Stream.Sum()
		assert.Error(t, err)
		assert.Zero(t, sum)
	})
}

func Test_parallelFloat64Stream_Average(t *testing.T) {
	testFloat64StreamAverage(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Collect(t *testing.T) {
	testFloat64StreamCollect(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Distinct(t *testing.T) {
	testFloat64StreamDistinct(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Filter(t *testing.T) {
	testFloat64StreamFilter(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_FlatMap(t *testing.T) {
	testFloat64StreamFlatMap(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Limit(t *testing.T) {
	testFloat64StreamLimit(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Map(t *testing.T) {
	testFloat64StreamMap(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_MapToInt(t *testing.T) {
	testFloat64StreamMapToInt(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_MapToObj(t *testing.T) {
	testFloat64StreamMapToObj(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Max(t *testing.T) {
	testFloat64StreamMax(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Min(t *testing.T) {
	testFloat64StreamMin(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Parallel(t *testing.T) {
	testFloat64StreamParallel(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Reduce(t *testing.T) {
	testFloat64StreamReduce(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Sequential(t *testing.T) {
	testFloat64StreamSequential(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Skip(t *testing.T) {
	testFloat64StreamSkip(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Sorted(t *testing.T) {
	testFloat64StreamSorted(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Sum(t *testing.T) {
	testFloat64StreamSum(t, NewParallelFloat64Stream)
}

func Test_parallelFloat64Stream_Err(t *testing.T) {
	testFloat64StreamErr(t, NewParallelFloat64Stream)
}

func Test_serialFloat64Stream_Average(t *testing.T) {
	testFloat64StreamAverage(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Collect(t *testing.T) {
	testFloat64StreamCollect(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Distinct(t *testing.T) {
	testFloat64StreamDistinct(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Filter(t *testing.T) {
	testFloat64StreamFilter(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_FlatMap(t *testing.T) {
	testFloat64StreamFlatMap(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Limit(t *testing.T) {
	testFloat64StreamLimit(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Map(t *testing.T) {
	testFloat64StreamMap(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_MapToInt(t *testing.T) {
	testFloat64StreamMapToInt(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_MapToObj(t *testing.T) {
	testFloat64StreamMapToObj(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Max(t *testing.T) {
	testFloat64StreamMax(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Min(t *testing.T) {
	testFloat64StreamMin(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Parallel(t *testing.T) {
	testFloat64StreamParallel(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Reduce(t *testing.T) {
	testFloat64StreamReduce(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Sequential(t *testing.T) {
	testFloat64StreamSequential(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Skip(t *testing.T) {
	testFloat64StreamSkip(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Sorted(t *testing.T) {
	testFloat64StreamSorted(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Sum(t *testing.T) {
	testFloat64StreamSum(t, NewSequentialFloat64Stream)
}

func Test_serialFloat64Stream_Err(t *testing.T) {
	testFloat64StreamErr(t, NewSequentialFloat64Stream)
}

func testFloat64StreamAverage(t *testing.T, stream func([]float64) Float64Stream) {
	t.Run("test no element", func(t *testing.T) {
		result, err := stream([]float64{}).Average()
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	tests := []struct {
		elements []float64
		expect   float64
	}{
		{[]float64{1}, 1},
		{[]float64{1, 2}, 1.5},
		{[]float64{1, 2, 3}, 2},
		{[]float64{1, 2, 3, 4}, 2.5},
		{[]float64{1, 2, 3, 4, 5}, 3},
		{[]float64{1, 2, 3, 4, 5, 3}, 3},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("test %d elements", len(tt.elements)), func(t *testing.T) {
			result, err := stream(tt.elements).Average()
			assert.NoError(t, err)
			assert.EqualValues(t, tt.expect, *result)
		})
	}
}

func testFloat64StreamCollect(t *testing.T, stream func([]float64) Float64Stream) {
	tests := []struct {
		elements []float64
		expect   []float64
	}{
		{[]float64{}, []float64{}},
		{[]float64{1}, []float64{1}},
		{[]float64{1, 2}, []float64{1, 2}},
		{[]float64{1, 2, 3}, []float64{1, 2, 3}},
		{[]float64{1, 2, 3, 4}, []float64{1, 2, 3, 4}},
		{[]float64{1, 2, 3, 4, 5, 6}, []float64{1, 2, 3, 4, 5, 6}},
		{[]float64{1, 2, 3, 4, 5, 6, 7}, []float64{1, 2, 3, 4, 5, 6, 7}},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8}, []float64{1, 2, 3, 4, 5, 6, 7, 8}},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9}, []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("test %d elements", len(tt.elements)), func(t *testing.T) {
			result, err := stream(tt.elements).Collect()
			assert.NoError(t, err)
			assertFloat64SliceEquals(t, tt.expect, result)
		})
	}
}

func testFloat64StreamDistinct(t *testing.T, stream func([]float64) Float64Stream) {
	type testCase struct {
		elements []float64
		expect   []float64
	}
	tests := []struct {
		name      string
		testCases []testCase
	}{
		{
			name: "test no duplicate",
			testCases: []testCase{
				{[]float64{}, []float64{}},
				{[]float64{1}, []float64{1}},
				{[]float64{1, 2}, []float64{1, 2}},
				{[]float64{1, 2, 3}, []float64{1, 2, 3}},
				{[]float64{1, 2, 3, 4}, []float64{1, 2, 3, 4}},
				{[]float64{1, 2, 3, 4, 5}, []float64{1, 2, 3, 4, 5}},
			},
		},
		{
			name: "test duplicate",
			testCases: []testCase{
				{[]float64{1, 1}, []float64{1}},
				{[]float64{1, 2, 1}, []float64{1, 2}},
				{[]float64{1, 2, 3, 3}, []float64{1, 2, 3}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, tCase := range tt.testCases {
				s := stream(tCase.elements)
				parallel := s.IsParallel()
				s = s.Distinct()
				assert.Equal(t, parallel, s.IsParallel())
				result, err := s.Collect()
				assert.NoError(t, err)
				assertFloat64SliceEquals(t, tCase.expect, result)
			}
		})
	}
}

func testFloat64StreamErr(t *testing.T, stream func([]float64) Float64Stream) {
	assert.NoError(t, stream(nil).Err())
}

func testFloat64StreamFilter(t *testing.T, stream func([]float64) Float64Stream) {
	type testCase struct {
		elements, expect []float64
	}
	tests := []struct {
		name      string
		testCases []*testCase
	}{
		{
			name:      "test no element",
			testCases: []*testCase{{[]float64{}, []float64{}}},
		},
		{
			name: "test no filtered",
			testCases: []*testCase{
				{[]float64{1}, []float64{1}},
				{[]float64{1, 1}, []float64{1, 1}},
				{[]float64{1, 1, 1}, []float64{1, 1, 1}},
			},
		},
		{
			name: "test partial filtered",
			testCases: []*testCase{
				{[]float64{1, 2, 3, 4}, []float64{1}},
				{[]float64{2, 3, 1, 4}, []float64{1}},
				{[]float64{2, 3, 4, 1}, []float64{1}},
			},
		},
		{
			name: "test all filtered",
			testCases: []*testCase{
				{[]float64{2}, []float64{}},
				{[]float64{2, 3}, []float64{}},
				{[]float64{2, 4, 5}, []float64{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, tCase := range tt.testCases {
				s := stream(tCase.elements)
				parallel := s.IsParallel()
				s = s.Filter(func(val float64) (match bool) {
					return val == 1
				})
				assert.Equal(t, parallel, s.IsParallel())
				result, err := s.Collect()
				assert.NoError(t, err)
				assertFloat64SliceEquals(t, tCase.expect, result)
			}
		})
	}
}

func testFloat64StreamFlatMap(t *testing.T, stream func([]float64) Float64Stream) {
	t.Run("test no element", func(t *testing.T) {
		s := stream([]float64{})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val float64) Float64Stream {
			return testSerialErrFloat64Stream
		})
		assert.Equal(t, parallel, s.IsParallel())
		res, err := s.Collect()
		assert.NoError(t, err)
		assert.Len(t, res, 0)
	})

	t.Run("test FlatMap sequentialIntStream normal", func(t *testing.T) {
		s := stream([]float64{1, 2, 3})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val float64) Float64Stream {
			return NewSequentialFloat64Stream([]float64{val, -val})
		})
		assert.Equal(t, parallel, s.IsParallel())
		res, err := s.Collect()
		assert.NoError(t, err)
		expect := []float64{1, -1, 2, -2, 3, -3}
		assertFloat64SliceEquals(t, expect, res)
	})

	t.Run("test FlatMap parallelIntStream normal", func(t *testing.T) {
		s := stream([]float64{1, 2, 3})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val float64) Float64Stream {
			return NewParallelFloat64Stream([]float64{val, -val})
		})
		assert.Equal(t, parallel, s.IsParallel())
		res, err := s.Collect()
		assert.NoError(t, err)
		expect := []float64{1, -1, 2, -2, 3, -3}
		assertFloat64SliceEquals(t, expect, res)
	})

	t.Run("test error", func(t *testing.T) {
		s := stream([]float64{1, 2, 3})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val float64) Float64Stream {
			if val == 1 {
				return testSerialErrFloat64Stream
			}
			return NewSequentialFloat64Stream([]float64{val})
		})
		assert.Equal(t, parallel, s.IsParallel())
		res, err := s.Collect()
		assert.Error(t, err)
		assert.Len(t, res, 0)
	})
}

func testFloat64StreamLimit(t *testing.T, stream func([]float64) Float64Stream) {
	t.Run("test maxSize is negative", func(t *testing.T) {
		s := stream([]float64{})
		parallel := s.IsParallel()
		s = s.Limit(-1)
		assert.Equal(t, parallel, s.IsParallel())
		assert.Error(t, s.Err())
	})
	type testCase struct {
		maxSize  int
		elements []float64
		expect   []float64
	}
	tests := []struct {
		name      string
		testCases []*testCase
	}{
		{
			name: "test maxSize > len(elements)",
			testCases: []*testCase{
				{100, []float64{}, []float64{}},
				{100, []float64{1, 2, 3}, []float64{1, 2, 3}},
			},
		},
		{
			name: "test maxSize = len(elements)",
			testCases: []*testCase{
				{0, []float64{}, []float64{}},
				{3, []float64{1, 2, 3}, []float64{1, 2, 3}},
			},
		},
		{
			name: "test maxSize < len(elements)",
			testCases: []*testCase{
				{0, []float64{1, 2, 3}, []float64{}},
				{2, []float64{1, 2, 3}, []float64{1, 2}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, tCase := range tt.testCases {
				s := stream(tCase.elements)
				parallel := s.IsParallel()
				s = s.Limit(tCase.maxSize)
				assert.Equal(t, parallel, s.IsParallel())
				result, err := s.Collect()
				assert.NoError(t, err)
				assertFloat64SliceEquals(t, tCase.expect, result)
			}
		})
	}
}

func testFloat64StreamMap(t *testing.T, stream func([]float64) Float64Stream) {
	tests := []struct {
		elements []float64
		expect   []float64
	}{
		{[]float64{}, []float64{}},
		{[]float64{1}, []float64{2}},
		{[]float64{1, 2}, []float64{2, 4}},
		{[]float64{1, 2, 3}, []float64{2, 4, 6}},
		{[]float64{1, 2, 3, 4}, []float64{2, 4, 6, 8}},
		{[]float64{1, 2, 3, 4, 5}, []float64{2, 4, 6, 8, 10}},
		{[]float64{1, 2, 3, 4, 5, 6}, []float64{2, 4, 6, 8, 10, 12}},
		{[]float64{1, 2, 3, 4, 5, 6, 7}, []float64{2, 4, 6, 8, 10, 12, 14}},
	}

	for _, tt := range tests {
		s := stream(tt.elements)
		parallel := s.IsParallel()
		s = s.Map(func(src float64) (dest float64) {
			return src * 2
		})
		assert.Equal(t, parallel, s.IsParallel())
		result, err := s.Collect()
		assert.NoError(t, err)
		assertFloat64SliceEquals(t, tt.expect, result)
	}
}

func testFloat64StreamMapToObj(t *testing.T, stream func([]float64) Float64Stream) {
	type testObj struct {
		val float64
	}
	tests := [][]float64{
		{},
		{1},
		{1, 2},
		{1, 2, 3},
	}
	for _, tt := range tests {
		s := stream(tt)
		parallel := s.IsParallel()
		newS := s.MapToObj(func(src float64) (dest interface{}) {
			return &testObj{src}
		})
		assert.Equal(t, parallel, newS.IsParallel())
		var dest []*testObj
		assert.NoError(t, newS.Collect(&dest))
		var actual []float64
		for _, obj := range dest {
			actual = append(actual, obj.val)
		}
		assertFloat64SliceEquals(t, tt, actual)
	}
}

func testFloat64StreamMax(t *testing.T, stream func([]float64) Float64Stream) {
	tests := []struct {
		elements []float64
		expect   *float64
	}{
		{[]float64{}, nil},
		{[]float64{1}, float64Ptr(1)},
		{[]float64{1, 2}, float64Ptr(2)},
		{[]float64{1, 3, 2}, float64Ptr(3)},
	}
	for _, tt := range tests {
		result, err := stream(tt.elements).Max()
		assert.NoError(t, err)
		if tt.expect == nil {
			assert.Nil(t, result)
		} else {
			assert.Equal(t, *tt.expect, *result)
		}
	}
}

func testFloat64StreamMin(t *testing.T, stream func([]float64) Float64Stream) {
	tests := []struct {
		elements []float64
		expect   *float64
	}{
		{[]float64{}, nil},
		{[]float64{1}, float64Ptr(1)},
		{[]float64{2, 1}, float64Ptr(1)},
		{[]float64{3, 1, 2}, float64Ptr(1)},
	}
	for _, tt := range tests {
		result, err := stream(tt.elements).Min()
		assert.NoError(t, err)
		if tt.expect == nil {
			assert.Nil(t, result)
		} else {
			assert.Equal(t, *tt.expect, *result)
		}
	}
}

func testFloat64StreamParallel(t *testing.T, stream func([]float64) Float64Stream) {
	t.Run("test empty", func(t *testing.T) {
		assert.True(t, stream(nil).Parallel().IsParallel())
	})
	t.Run("test 1 element", func(t *testing.T) {
		assert.True(t, stream([]float64{1}).Parallel().IsParallel())
	})
}

func testFloat64StreamReduce(t *testing.T, stream func([]float64) Float64Stream) {
	tests := []struct {
		elements []float64
		expect   *float64
	}{
		{[]float64{}, nil},
		{[]float64{1}, float64Ptr(1)},
		{[]float64{1, 2}, float64Ptr(3)},
		{[]float64{1, 2, 3}, float64Ptr(6)},
		{[]float64{1, 2, 3, 4}, float64Ptr(10)},
		{[]float64{1, 2, 3, 4, 5}, float64Ptr(15)},
		{[]float64{1, 2, 3, 4, 5, 6}, float64Ptr(21)},
		{[]float64{1, 2, 3, 4, 5, 6, 7}, float64Ptr(28)},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8}, float64Ptr(36)},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9}, float64Ptr(45)},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, float64Ptr(55)},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("test %d elements", len(tt.elements)), func(t *testing.T) {
			result, err := stream(tt.elements).Reduce(func(a, b float64) (c float64) {
				return a + b
			})
			assert.NoError(t, err)
			if tt.expect == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, *tt.expect, *result)
			}
		})
	}
}

func testFloat64StreamSequential(t *testing.T, stream func([]float64) Float64Stream) {
	t.Run("test empty", func(t *testing.T) {
		assert.False(t, stream(nil).Sequential().IsParallel())
	})
	t.Run("test 1 element", func(t *testing.T) {
		assert.False(t, stream([]float64{1}).Sequential().IsParallel())
	})
}

func testFloat64StreamSorted(t *testing.T, stream func([]float64) Float64Stream) {
	at := assert.New(t)
	type testCase struct {
		elements []float64
		expect   []float64
	}
	tests := []struct {
		name      string
		testCases []*testCase
	}{
		{
			name: "test sorted",
			testCases: []*testCase{
				{[]float64{}, []float64{}},
				{[]float64{1}, []float64{1}},
				{[]float64{1, 2}, []float64{1, 2}},
				{[]float64{1, 2, 3}, []float64{1, 2, 3}},
				{[]float64{1, 2, 3, 4}, []float64{1, 2, 3, 4}},
				{[]float64{1, 2, 3, 4, 5}, []float64{1, 2, 3, 4, 5}},
			},
		},
		{
			name: "test unsorted",
			testCases: []*testCase{
				{[]float64{1, 0}, []float64{0, 1}},
				{[]float64{0, 3, 1}, []float64{0, 1, 3}},
				{[]float64{1, 0, 3}, []float64{0, 1, 3}},
				{[]float64{1, 3, 0}, []float64{0, 1, 3}},
				{[]float64{3, 0, 1}, []float64{0, 1, 3}},
				{[]float64{3, 1, 0}, []float64{0, 1, 3}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, tCase := range tt.testCases {
				s := stream(tCase.elements)
				parallel := s.IsParallel()
				s = s.Sorted()
				assert.Equal(t, parallel, s.IsParallel())
				res, err := s.Collect()
				at.NoError(err)
				assertFloat64SliceEquals(t, tCase.expect, res)
			}
		})
	}
}

func testFloat64StreamSkip(t *testing.T, stream func([]float64) Float64Stream) {
	t.Run("test n is negative", func(t *testing.T) {
		assert.Error(t, stream([]float64{}).Skip(-1).Err())
	})
	tests := []struct {
		name     string
		elements []float64
		n        int
		expect   []float64
	}{
		{
			name:     "test n is 0",
			elements: []float64{1, 2, 3},
			n:        0,
			expect:   []float64{1, 2, 3},
		},
		{
			name:     "test n greater than len(elements)",
			elements: []float64{1, 2, 3},
			n:        4,
			expect:   []float64{},
		},
		{
			name:     "test n equals to len(elements)",
			elements: []float64{1, 2, 3},
			n:        3,
			expect:   []float64{},
		},
		{
			name:     "test n less than len(elements)",
			elements: []float64{1, 2, 3},
			n:        2,
			expect:   []float64{3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stream(tt.elements)
			parallel := s.IsParallel()
			s = s.Skip(tt.n)
			assert.Equal(t, parallel, s.IsParallel())
			res, err := s.Collect()
			assert.NoError(t, err)
			assertFloat64SliceEquals(t, tt.expect, res)
		})
	}
}

func testFloat64StreamSum(t *testing.T, stream func([]float64) Float64Stream) {
	tests := []struct {
		elements []float64
		expect   float64
	}{
		{[]float64{}, 0},
		{[]float64{1}, 1},
		{[]float64{1, 2}, 3},
		{[]float64{1, 2, 3}, 6},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("test %d elements", len(tt.elements)), func(t *testing.T) {
			sum, err := stream(tt.elements).Sum()
			assert.NoError(t, err)
			assert.Equal(t, tt.expect, sum)
		})
	}
}

func testFloat64StreamMapToInt(t *testing.T, stream func([]float64) Float64Stream) {
	tests := []struct {
		elements []float64
		expect   []int
	}{
		{[]float64{}, []int{}},
		{[]float64{1}, []int{1}},
		{[]float64{1, 2}, []int{1, 2}},
		{[]float64{1, 2, 3}, []int{1, 2, 3}},
	}
	for _, tt := range tests {
		s := stream(tt.elements)
		parallel := s.IsParallel()
		newS := s.MapToInt(func(src float64) (dest int) {
			return int(src)
		})
		assert.Equal(t, parallel, newS.IsParallel())
		dest, err := newS.Collect()
		assert.NoError(t, err)
		assertSliceEquals(t, tt.expect, dest)
	}
}

func assertFloat64SliceEquals(t *testing.T, expect, actual []float64) {
	equals := func(a, b []float64) bool {
		if len(a) != len(b) {
			return false
		}
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	assert.True(t, equals(expect, actual), "expect=%v, actual=%v", expect, actual)
}

func float64Ptr(val float64) *float64 {
	return &val
}

func TestNewSerialFloat64Stream(t *testing.T) {
	tests := []struct {
		name     string
		elements []float64
	}{
		{"test nil", nil},
		{"test no element", []float64{}},
		{"test 1 element", []float64{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSequentialFloat64Stream(tt.elements)
			assert.NoError(t, s.Err())
			assert.False(t, s.IsParallel())
		})
	}
}

func TestNewParallelFloat64Stream(t *testing.T) {
	tests := []struct {
		name     string
		elements []float64
	}{
		{"test nil", nil},
		{"test no element", []float64{}},
		{"test 1 element", []float64{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewParallelFloat64Stream(tt.elements)
			assert.NoError(t, s.Err())
			assert.True(t, s.IsParallel())
		})
	}
}
