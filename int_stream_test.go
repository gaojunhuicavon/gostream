package gostream

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	testErrIntStream         = &errIntStream{err: errors.New("")}
	testParallelErrIntStream = &errIntStream{err: errors.New(""), parallel: true}
)

func TestConcatIntStream(t *testing.T) {
	t.Run("test a error", func(t *testing.T) {
		s := ConcatIntStream(&errIntStream{err: errors.New("")}, &sequentialIntStream{})
		assert.Error(t, s.Err())
	})
	t.Run("test b error", func(t *testing.T) {
		s := ConcatIntStream(&sequentialIntStream{}, &errIntStream{err: errors.New("")})
		assert.Error(t, s.Err())
	})
	t.Run("test both empty", func(t *testing.T) {
		res, err := ConcatIntStream(&sequentialIntStream{}, &sequentialIntStream{}).Collect()
		assert.NoError(t, err)
		assert.Len(t, res, 0)
	})
	t.Run("test a empty", func(t *testing.T) {
		res, err := ConcatIntStream(&sequentialIntStream{}, &sequentialIntStream{[]int{1}}).Collect()
		assert.NoError(t, err)
		expect := []int{1}
		assertSliceEquals(t, expect, res)
	})
	t.Run("test b empty", func(t *testing.T) {
		res, err := ConcatIntStream(&sequentialIntStream{[]int{1}}, &sequentialIntStream{}).Collect()
		assert.NoError(t, err)
		expect := []int{1}
		assertSliceEquals(t, expect, res)
	})
	t.Run("test normal", func(t *testing.T) {
		res, err := ConcatIntStream(&sequentialIntStream{[]int{1}}, &sequentialIntStream{[]int{2}}).Collect()
		assert.NoError(t, err)
		expect := []int{1, 2}
		assertSliceEquals(t, expect, res)
	})
}

func Test_errIntStream_Average(t *testing.T) {
	ans, err := testErrIntStream.Average()
	assert.Nil(t, ans)
	assert.Error(t, err)
}

func Test_errIntStream_Collect(t *testing.T) {
	ans, err := testErrIntStream.Collect()
	assert.Nil(t, ans)
	assert.Error(t, err)
}

func Test_errIntStream_Distinct(t *testing.T) {
	ans, err := testErrIntStream.Distinct().Collect()
	assert.Nil(t, ans)
	assert.Error(t, err)
}

func Test_errIntStream_Filter(t *testing.T) {
	s := testErrIntStream.Filter(func(val int) (match bool) {
		return false
	})
	assert.Error(t, s.Err())
	assert.Same(t, testErrIntStream, s)
}

func Test_errIntStream_FlatMap(t *testing.T) {
	s := testErrIntStream.FlatMap(func(val int) IntStream {
		return &sequentialIntStream{elements: []int{1, 2, 3}}
	})
	assert.Error(t, s.Err())
	assert.Same(t, testErrIntStream, s)
}

func Test_errIntStream_IsParallel(t *testing.T) {
	assert.True(t, (&errIntStream{err: errors.New(""), parallel: true}).IsParallel())
	assert.False(t, (&errIntStream{err: errors.New(""), parallel: false}).IsParallel())
}

func Test_errIntStream_Limit(t *testing.T) {
	s := testErrIntStream.Limit(1)
	assert.Error(t, s.Err())
	assert.Same(t, testErrIntStream, s)
}

func Test_errIntStream_Map(t *testing.T) {
	s := testErrIntStream.Map(func(src int) (dest int) {
		return src
	})
	assert.Error(t, s.Err())
	assert.Same(t, testErrIntStream, s)
}

func Test_errIntStream_Max(t *testing.T) {
	res, err := testErrIntStream.Max()
	assert.Error(t, err)
	assert.Nil(t, res)
}

func Test_errIntStream_Min(t *testing.T) {
	res, err := testErrIntStream.Min()
	assert.Error(t, err)
	assert.Nil(t, res)
}

func Test_errIntStream_Parallel(t *testing.T) {
	at := assert.New(t)
	t.Run("test sequential to parallel", func(t *testing.T) {
		src := &errIntStream{err: errors.New(""), parallel: false}
		dest := src.Parallel()
		at.True(dest.IsParallel())
		at.Error(dest.Err())
		at.NotSame(src, dest)
	})

	t.Run("test parallel to parallel", func(t *testing.T) {
		src := &errIntStream{err: errors.New(""), parallel: true}
		dest := src.Parallel()
		at.True(dest.IsParallel())
		at.Error(dest.Err())
		at.Same(src, dest)
	})
}

func Test_errIntStream_Reduce(t *testing.T) {
	at := assert.New(t)
	res, err := testErrIntStream.Reduce(func(a, b int) (c int) {
		return a + b
	})
	at.Error(err)
	at.Nil(res)
}

func Test_errIntStream_Sequential(t *testing.T) {
	at := assert.New(t)
	t.Run("to sequential to sequential", func(t *testing.T) {
		src := &errIntStream{err: errors.New(""), parallel: false}
		dest := src.Sequential()
		at.False(dest.IsParallel())
		at.Error(dest.Err())
		at.Same(src, dest)
	})
	t.Run("to parallel to sequential", func(t *testing.T) {
		src := &errIntStream{err: errors.New(""), parallel: true}
		dest := src.Sequential()
		at.False(dest.IsParallel())
		at.Error(dest.Err())
		at.NotSame(src, dest)
	})
}

func Test_errIntStream_Skip(t *testing.T) {
	at := assert.New(t)
	s := testErrIntStream.Skip(1)
	at.Error(s.Err())
}

func Test_errIntStream_Sorted(t *testing.T) {
	at := assert.New(t)
	s := testErrIntStream.Sorted()
	at.Error(s.Err())
}

func Test_errIntStream_MapToObj(t *testing.T) {
	at := assert.New(t)
	t.Run("test sequential", func(t *testing.T) {
		s := (&errIntStream{err: errors.New("")}).MapToObj(func(src int) (dest interface{}) {
			return &struct{}{}
		})
		var dest []interface{}
		err := s.Collect(&dest)
		at.Error(err)
		at.Empty(dest)
		at.False(s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := (&errIntStream{err: errors.New(""), parallel: true}).MapToObj(func(src int) (dest interface{}) {
			return &struct{}{}
		})
		var dest []interface{}
		err := s.Collect(&dest)
		at.Error(err)
		at.Empty(dest)
		at.True(s.IsParallel())
	})
}

func Test_sequentialIntStream_Average(t *testing.T) {
	testIntStreamAverage(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Collect(t *testing.T) {
	testIntStreamCollect(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Distinct(t *testing.T) {
	testIntStreamDistinct(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Filter(t *testing.T) {
	testIntStreamFilter(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_FlatMap(t *testing.T) {
	testIntStreamFlatMap(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_IsParallel(t *testing.T) {
	assert.False(t, NewSequentialIntStream([]int{}).IsParallel())
}

func Test_sequentialIntStream_Limit(t *testing.T) {
	testIntStreamLimit(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Map(t *testing.T) {
	testIntStreamMap(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Max(t *testing.T) {
	testIntStreamMax(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Min(t *testing.T) {
	testIntStreamMin(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Parallel(t *testing.T) {
	testIntStreamParallel(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Reduce(t *testing.T) {
	testIntStreamReduce(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Sequential(t *testing.T) {
	testIntStreamSequential(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Skip(t *testing.T) {
	testIntStreamSkip(t, func(ints []int) IntStream { return &sequentialIntStream{ints} })
}

func Test_sequentialIntStream_Sorted(t *testing.T) {
	testIntStreamSorted(t, NewSequentialIntStream)
}

func Test_sequentialIntStream_Err(t *testing.T) {
	testIntStreamErr(t, NewSequentialIntStream([]int{}))
}

func Test_sequentialIntStream_MapToObj(t *testing.T) {
	testIntStreamMapToObj(t, NewSequentialIntStream)
}

func Test_parallelIntStream_IsParallel(t *testing.T) {
	assert.True(t, NewParallelIntStream([]int{}).IsParallel())
}

func Test_parallelIntStream_Average(t *testing.T) {
	testIntStreamAverage(t, NewParallelIntStream)
}

func Test_parallelIntStream_Collect(t *testing.T) {
	testIntStreamCollect(t, NewParallelIntStream)
}

func Test_parallelIntStream_Distinct(t *testing.T) {
	testIntStreamDistinct(t, NewParallelIntStream)
}

func Test_parallelIntStream_Err(t *testing.T) {
	testIntStreamErr(t, NewParallelIntStream([]int{}))
}

func Test_parallelIntStream_Filter(t *testing.T) {
	testIntStreamFilter(t, NewParallelIntStream)
}

func Test_parallelIntStream_FlatMap(t *testing.T) {
	testIntStreamFlatMap(t, NewParallelIntStream)
}

func Test_parallelIntStream_Limit(t *testing.T) {
	testIntStreamLimit(t, NewParallelIntStream)
}

func Test_parallelIntStream_Map(t *testing.T) {
	testIntStreamMap(t, NewParallelIntStream)
}

func Test_parallelIntStream_MapToObj(t *testing.T) {
	testIntStreamMapToObj(t, NewParallelIntStream)
}

func Test_parallelIntStream_Max(t *testing.T) {
	testIntStreamMax(t, NewParallelIntStream)
}

func Test_parallelIntStream_Min(t *testing.T) {
	testIntStreamMin(t, NewParallelIntStream)
}

func Test_parallelIntStream_Parallel(t *testing.T) {
	testIntStreamParallel(t, NewParallelIntStream)
}

func Test_parallelIntStream_Reduce(t *testing.T) {
	testIntStreamReduce(t, NewParallelIntStream)
}

func Test_parallelIntStream_Sequential(t *testing.T) {
	testIntStreamSequential(t, NewParallelIntStream)
}

func Test_parallelIntStream_Skip(t *testing.T) {
	testIntStreamSkip(t, NewParallelIntStream)
}

func Test_parallelIntStream_Sorted(t *testing.T) {
	testIntStreamSorted(t, NewParallelIntStream)
}

func testIntStreamAverage(t *testing.T, stream func([]int) IntStream) {
	t.Run("test no element", func(t *testing.T) {
		result, err := stream([]int{}).Average()
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	tests := []struct {
		elements []int
		expect   float64
	}{
		{[]int{1}, 1},
		{[]int{1, 2}, 1.5},
		{[]int{1, 2, 3}, 2},
		{[]int{1, 2, 3, 4}, 2.5},
		{[]int{1, 2, 3, 4, 5}, 3},
		{[]int{1, 2, 3, 4, 5, 3}, 3},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("test %d elements", len(tt.elements)), func(t *testing.T) {
			result, err := stream(tt.elements).Average()
			assert.NoError(t, err)
			assert.EqualValues(t, tt.expect, *result)
		})
	}
}

func testIntStreamCollect(t *testing.T, stream func([]int) IntStream) {
	tests := []struct {
		elements []int
		expect   []int
	}{
		{[]int{}, []int{}},
		{[]int{1}, []int{1}},
		{[]int{1, 2}, []int{1, 2}},
		{[]int{1, 2, 3}, []int{1, 2, 3}},
		{[]int{1, 2, 3, 4}, []int{1, 2, 3, 4}},
		{[]int{1, 2, 3, 4, 5, 6}, []int{1, 2, 3, 4, 5, 6}},
		{[]int{1, 2, 3, 4, 5, 6, 7}, []int{1, 2, 3, 4, 5, 6, 7}},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, []int{1, 2, 3, 4, 5, 6, 7, 8}},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8, 9}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("test %d elements", len(tt.elements)), func(t *testing.T) {
			result, err := stream(tt.elements).Collect()
			assert.NoError(t, err)
			assertSliceEquals(t, tt.expect, result)
		})
	}
}

func testIntStreamDistinct(t *testing.T, stream func([]int) IntStream) {
	type testCase struct {
		elements []int
		expect   []int
	}
	tests := []struct {
		name      string
		testCases []testCase
	}{
		{
			name: "test no duplicate",
			testCases: []testCase{
				{[]int{}, []int{}},
				{[]int{1}, []int{1}},
				{[]int{1, 2}, []int{1, 2}},
				{[]int{1, 2, 3}, []int{1, 2, 3}},
				{[]int{1, 2, 3, 4}, []int{1, 2, 3, 4}},
				{[]int{1, 2, 3, 4, 5}, []int{1, 2, 3, 4, 5}},
			},
		},
		{
			name: "test duplicate",
			testCases: []testCase{
				{[]int{1, 1}, []int{1}},
				{[]int{1, 2, 1}, []int{1, 2}},
				{[]int{1, 2, 3, 3}, []int{1, 2, 3}},
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
				assertSliceEquals(t, tCase.expect, result)
			}
		})
	}
}

func testIntStreamErr(t *testing.T, stream IntStream) {
	assert.NoError(t, stream.Err())
}

func testIntStreamFilter(t *testing.T, stream func([]int) IntStream) {
	type testCase struct {
		elements, expect []int
	}
	tests := []struct {
		name      string
		testCases []*testCase
	}{
		{
			name:      "test no element",
			testCases: []*testCase{{[]int{}, []int{}}},
		},
		{
			name: "test no filtered",
			testCases: []*testCase{
				{[]int{1}, []int{1}},
				{[]int{1, 1}, []int{1, 1}},
				{[]int{1, 1, 1}, []int{1, 1, 1}},
			},
		},
		{
			name: "test partial filtered",
			testCases: []*testCase{
				{[]int{1, 2, 3, 4}, []int{1}},
				{[]int{2, 3, 1, 4}, []int{1}},
				{[]int{2, 3, 4, 1}, []int{1}},
			},
		},
		{
			name: "test all filtered",
			testCases: []*testCase{
				{[]int{2}, []int{}},
				{[]int{2, 3}, []int{}},
				{[]int{2, 4, 5}, []int{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, tCase := range tt.testCases {
				s := stream(tCase.elements)
				parallel := s.IsParallel()
				s = s.Filter(func(val int) (keep bool) {
					return val == 1
				})
				assert.Equal(t, parallel, s.IsParallel())
				result, err := s.Collect()
				assert.NoError(t, err)
				assertSliceEquals(t, tCase.expect, result)
			}
		})
	}
}

func testIntStreamFlatMap(t *testing.T, stream func([]int) IntStream) {
	t.Run("test no element", func(t *testing.T) {
		s := stream([]int{})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val int) IntStream {
			return &errIntStream{err: errors.New("")}
		})
		assert.Equal(t, parallel, s.IsParallel())
		res, err := s.Collect()
		assert.NoError(t, err)
		assert.Len(t, res, 0)
	})

	t.Run("test FlatMap sequentialIntStream normal", func(t *testing.T) {
		s := stream([]int{1, 2, 3})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val int) IntStream {
			return NewSequentialIntStream([]int{val, -val})
		})
		assert.Equal(t, parallel, s.IsParallel())
		res, err := s.Collect()
		assert.NoError(t, err)
		expect := []int{1, -1, 2, -2, 3, -3}
		assertSliceEquals(t, expect, res)
	})

	t.Run("test FlatMap parallelIntStream normal", func(t *testing.T) {
		s := stream([]int{1, 2, 3})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val int) IntStream {
			return NewParallelIntStream([]int{val, -val})
		})
		assert.Equal(t, parallel, s.IsParallel())
		res, err := s.Collect()
		assert.NoError(t, err)
		expect := []int{1, -1, 2, -2, 3, -3}
		assertSliceEquals(t, expect, res)
	})

	t.Run("test error", func(t *testing.T) {
		s := stream([]int{1, 2, 3})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val int) IntStream {
			if val == 1 {
				return &errIntStream{err: errors.New("")}
			}
			return NewSequentialIntStream([]int{val})
		})
		assert.Equal(t, parallel, s.IsParallel())
		res, err := s.Collect()
		assert.Error(t, err)
		assert.Len(t, res, 0)
	})
}

func testIntStreamLimit(t *testing.T, stream func([]int) IntStream) {
	t.Run("test maxSize is negative", func(t *testing.T) {
		s := stream([]int{})
		parallel := s.IsParallel()
		s = s.Limit(-1)
		assert.Equal(t, parallel, s.IsParallel())
		assert.Error(t, s.Err())
	})
	type testCase struct {
		maxSize  int
		elements []int
		expect   []int
	}
	tests := []struct {
		name      string
		testCases []*testCase
	}{
		{
			name: "test maxSize > len(elements)",
			testCases: []*testCase{
				{100, []int{}, []int{}},
				{100, []int{1, 2, 3}, []int{1, 2, 3}},
			},
		},
		{
			name: "test maxSize = len(elements)",
			testCases: []*testCase{
				{0, []int{}, []int{}},
				{3, []int{1, 2, 3}, []int{1, 2, 3}},
			},
		},
		{
			name: "test maxSize < len(elements)",
			testCases: []*testCase{
				{0, []int{1, 2, 3}, []int{}},
				{2, []int{1, 2, 3}, []int{1, 2}},
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
				assertSliceEquals(t, tCase.expect, result)
			}
		})
	}
}

func testIntStreamMap(t *testing.T, stream func([]int) IntStream) {
	tests := []struct {
		elements []int
		expect   []int
	}{
		{[]int{}, []int{}},
		{[]int{1}, []int{2}},
		{[]int{1, 2}, []int{2, 4}},
		{[]int{1, 2, 3}, []int{2, 4, 6}},
		{[]int{1, 2, 3, 4}, []int{2, 4, 6, 8}},
		{[]int{1, 2, 3, 4, 5}, []int{2, 4, 6, 8, 10}},
		{[]int{1, 2, 3, 4, 5, 6}, []int{2, 4, 6, 8, 10, 12}},
		{[]int{1, 2, 3, 4, 5, 6, 7}, []int{2, 4, 6, 8, 10, 12, 14}},
	}

	for _, tt := range tests {
		s := stream(tt.elements)
		parallel := s.IsParallel()
		s = s.Map(func(src int) (dest int) {
			return src * 2
		})
		assert.Equal(t, parallel, s.IsParallel())
		result, err := s.Collect()
		assert.NoError(t, err)
		assertSliceEquals(t, tt.expect, result)
	}
}

func testIntStreamMapToObj(t *testing.T, stream func([]int) IntStream) {
	type testObj struct {
		val int
	}
	tests := [][]int{
		{},
		{1},
		{1, 2},
		{1, 2, 3},
	}
	for _, tt := range tests {
		s := stream(tt)
		parallel := s.IsParallel()
		newS := s.MapToObj(func(src int) (dest interface{}) {
			return &testObj{src}
		})
		assert.Equal(t, parallel, newS.IsParallel())
		var dest []*testObj
		assert.NoError(t, newS.Collect(&dest))
		var actual []int
		for _, obj := range dest {
			actual = append(actual, obj.val)
		}
		assertSliceEquals(t, tt, actual)
	}
}

func testIntStreamMax(t *testing.T, stream func([]int) IntStream) {
	tests := []struct {
		elements []int
		expect   *int
	}{
		{[]int{}, nil},
		{[]int{1}, intPtr(1)},
		{[]int{1, 2}, intPtr(2)},
		{[]int{1, 3, 2}, intPtr(3)},
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

func testIntStreamMin(t *testing.T, stream func([]int) IntStream) {
	tests := []struct {
		elements []int
		expect   *int
	}{
		{[]int{}, nil},
		{[]int{1}, intPtr(1)},
		{[]int{2, 1}, intPtr(1)},
		{[]int{3, 1, 2}, intPtr(1)},
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

func testIntStreamParallel(t *testing.T, stream func([]int) IntStream) {
	t.Run("test empty", func(t *testing.T) {
		assert.True(t, stream(nil).Parallel().IsParallel())
	})
	t.Run("test 1 element", func(t *testing.T) {
		assert.True(t, stream([]int{1}).Parallel().IsParallel())
	})
}

func testIntStreamReduce(t *testing.T, stream func([]int) IntStream) {
	tests := []struct {
		elements []int
		expect   *int
	}{
		{[]int{}, nil},
		{[]int{1}, intPtr(1)},
		{[]int{1, 2}, intPtr(3)},
		{[]int{1, 2, 3}, intPtr(6)},
		{[]int{1, 2, 3, 4}, intPtr(10)},
		{[]int{1, 2, 3, 4, 5}, intPtr(15)},
		{[]int{1, 2, 3, 4, 5, 6}, intPtr(21)},
		{[]int{1, 2, 3, 4, 5, 6, 7}, intPtr(28)},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, intPtr(36)},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8, 9}, intPtr(45)},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, intPtr(55)},
	}
	for _, tt := range tests {
		result, err := stream(tt.elements).Reduce(func(a, b int) (c int) {
			return a + b
		})
		assert.NoError(t, err)
		if tt.expect == nil {
			assert.Nil(t, result)
		} else {
			assert.Equal(t, *tt.expect, *result)
		}
	}
}

func testIntStreamSequential(t *testing.T, stream func([]int) IntStream) {
	t.Run("test empty", func(t *testing.T) {
		assert.False(t, stream(nil).Sequential().IsParallel())
	})
	t.Run("test 1 element", func(t *testing.T) {
		assert.False(t, stream([]int{1}).Sequential().IsParallel())
	})
}

func testIntStreamSorted(t *testing.T, stream func([]int) IntStream) {
	at := assert.New(t)
	type testCase struct {
		elements []int
		expect   []int
	}
	tests := []struct {
		name      string
		testCases []*testCase
	}{
		{
			name: "test sorted",
			testCases: []*testCase{
				{[]int{}, []int{}},
				{[]int{1}, []int{1}},
				{[]int{1, 2}, []int{1, 2}},
				{[]int{1, 2, 3}, []int{1, 2, 3}},
				{[]int{1, 2, 3, 4}, []int{1, 2, 3, 4}},
				{[]int{1, 2, 3, 4, 5}, []int{1, 2, 3, 4, 5}},
			},
		},
		{
			name: "test unsorted",
			testCases: []*testCase{
				{[]int{1, 0}, []int{0, 1}},
				{[]int{0, 3, 1}, []int{0, 1, 3}},
				{[]int{1, 0, 3}, []int{0, 1, 3}},
				{[]int{1, 3, 0}, []int{0, 1, 3}},
				{[]int{3, 0, 1}, []int{0, 1, 3}},
				{[]int{3, 1, 0}, []int{0, 1, 3}},
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
				assertSliceEquals(t, tCase.expect, res)
			}
		})
	}
}

func testIntStreamSkip(t *testing.T, stream func([]int) IntStream) {
	t.Run("test n is negative", func(t *testing.T) {
		assert.Error(t, stream([]int{}).Skip(-1).Err())
	})
	tests := []struct {
		name     string
		elements []int
		n        int
		expect   []int
	}{
		{
			name:     "test n is 0",
			elements: []int{1, 2, 3},
			n:        0,
			expect:   []int{1, 2, 3},
		},
		{
			name:     "test n greater than len(elements)",
			elements: []int{1, 2, 3},
			n:        4,
			expect:   []int{},
		},
		{
			name:     "test n equals to len(elements)",
			elements: []int{1, 2, 3},
			n:        3,
			expect:   []int{},
		},
		{
			name:     "test n less than len(elements)",
			elements: []int{1, 2, 3},
			n:        2,
			expect:   []int{3},
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
			assertSliceEquals(t, tt.expect, res)
		})
	}
}

func intPtr(i int) *int {
	return &i
}

func Test_sequentialIntStream_MapToFloat64(t *testing.T) {
	testIntStreamMapToFloat64(t, NewSequentialIntStream)
}
func Test_parallelIntStream_MapToFloat64(t *testing.T) {
	testIntStreamMapToFloat64(t, NewParallelIntStream)
}

func testIntStreamMapToFloat64(t *testing.T, stream func([]int) IntStream) {
	tests := []struct {
		elements []int
		expect   []float64
	}{
		{[]int{}, []float64{}},
		{[]int{1}, []float64{2}},
		{[]int{1, 2}, []float64{2, 4}},
		{[]int{1, 2, 3}, []float64{2, 4, 6}},
		{[]int{1, 2, 3, 4}, []float64{2, 4, 6, 8}},
		{[]int{1, 2, 3, 4, 5}, []float64{2, 4, 6, 8, 10}},
		{[]int{1, 2, 3, 4, 5, 6}, []float64{2, 4, 6, 8, 10, 12}},
		{[]int{1, 2, 3, 4, 5, 6, 7}, []float64{2, 4, 6, 8, 10, 12, 14}},
	}

	for _, tt := range tests {
		s := stream(tt.elements)
		parallel := s.IsParallel()
		float64s := s.MapToFloat64(func(src int) (dest float64) {
			return float64(src) * 2
		})
		assert.Equal(t, parallel, float64s.IsParallel())
		result, err := float64s.Collect()
		assert.NoError(t, err)
		assertFloat64SliceEquals(t, tt.expect, result)
	}
}

func Test_errIntStream_MapToFloat64(t *testing.T) {
	t.Run("test sequential", func(t *testing.T) {
		s := testErrIntStream.MapToFloat64(func(src int) (dest float64) {
			return float64(src)
		})
		assert.Error(t, s.Err())
		assert.False(t, s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := testParallelErrIntStream.MapToFloat64(func(src int) (dest float64) {
			return float64(src)
		})
		assert.Error(t, s.Err())
		assert.True(t, s.IsParallel())
	})
}
