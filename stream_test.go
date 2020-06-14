package gostream

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

var testErrStream = &errStream{err: errors.New("")}

func TestConcatStream(t *testing.T) {
	at := assert.New(t)

	t.Run("test a error", func(t *testing.T) {
		s := ConcatStream(&errStream{err: errors.New("")}, NewSequentialStream([]int{1, 2, 3}))
		at.Error(s.Err())
	})

	t.Run("test b error", func(t *testing.T) {
		s := ConcatStream(NewSequentialStream([]int{1, 2, 3}), &errStream{err: errors.New("")})
		at.Error(s.Err())
	})

	t.Run("test normal", func(t *testing.T) {
		var dest []int
		err := ConcatStream(NewSequentialStream([]int{1, 2, 3}), NewSequentialStream([]int{4, 5, 6})).Collect(&dest)
		at.NoError(err)
		expect := []int{1, 2, 3, 4, 5, 6}
		assertSliceEquals(t, expect, dest)
	})
}

func Test_sequentialStream_Collect(t *testing.T) {
	testStreamCollect(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Distinct(t *testing.T) {
	testStreamDistinct(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Filter(t *testing.T) {
	testStreamFilter(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Map(t *testing.T) {
	testStreamMap(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Sorted(t *testing.T) {
	testStreamSorted(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Err(t *testing.T) {
	testStreamErr(t, newSequentialStreamForTest([]*element{}))
}

func Test_sequentialStream_FlatMap(t *testing.T) {
	testStreamFlatMap(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Limit(t *testing.T) {
	testStreamLimit(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Reduce(t *testing.T) {
	testStreamReduce(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Skip(t *testing.T) {
	testStreamSkip(t, newSequentialStreamForTest)
}

func Test_sequentialStream_IsParallel(t *testing.T) {
	assert.False(t, newSequentialStreamForTest([]*element{}).IsParallel())
}

func Test_sequentialStream_Sequential(t *testing.T) {
	testStreamSequential(t, newSequentialStreamForTest)
}

func Test_sequentialStream_Parallel(t *testing.T) {
	testStreamParallel(t, newSequentialStreamForTest)
}

func Test_sequentialStream_MapToInt(t *testing.T) {
	testStreamMapToInt(t, newSequentialStreamForTest)
}

func Test_errStream_Collect(t *testing.T) {
	err := testErrStream.Collect(1)
	assert.Error(t, err)
}

func Test_errStream_Distinct(t *testing.T) {
	s := testErrStream.Distinct(func(obj interface{}) int {
		return 0
	}, func(a, b interface{}) bool {
		return false
	})
	assert.Same(t, testErrStream, s)
}

func Test_errStream_Err(t *testing.T) {
	assert.Same(t, testErrStream.err, testErrStream.Err())
}

func Test_errStream_Filter(t *testing.T) {
	s := testErrStream.Filter(func(val interface{}) (keep bool) { return false })
	assert.Same(t, testErrStream, s)
}

func Test_errStream_FlatMap(t *testing.T) {
	s := testErrStream.FlatMap(func(val interface{}) Stream { return &sequentialStream{} })
	assert.Same(t, testErrStream, s)
}

func Test_errStream_Limit(t *testing.T) {
	s := testErrStream.Limit(1)
	assert.Same(t, testErrStream, s)
}

func Test_errStream_Map(t *testing.T) {
	s := testErrStream.Map(func(src interface{}) (dest interface{}) { return src })
	assert.Same(t, testErrStream, s)
}

func Test_errStream_Reduce(t *testing.T) {
	s, err := testErrStream.Reduce(func(a, b interface{}) (c interface{}) { return 1 })
	assert.Same(t, testErrStream.err, err)
	assert.Nil(t, s)
}

func Test_errStream_Skip(t *testing.T) {
	s := testErrStream.Skip(1)
	assert.Same(t, testErrStream, s)
}

func Test_errStream_Sorted(t *testing.T) {
	s := testErrStream.Sorted(func(a, b interface{}) bool {
		return false
	})
	assert.Same(t, testErrStream, s)
}

func Test_errStream_IsParallel(t *testing.T) {
	at := assert.New(t)
	at.False((&errStream{err: errors.New(""), parallel: false}).IsParallel())
	at.True((&errStream{err: errors.New(""), parallel: true}).IsParallel())
}

func Test_errStream_Sequential(t *testing.T) {
	at := assert.New(t)
	t.Run("test sequential to sequential", func(t *testing.T) {
		src := &errStream{err: errors.New(""), parallel: false}
		dest := src.Sequential()
		at.Error(dest.Err())
		at.False(dest.IsParallel())
		at.Same(src, dest)
	})
	t.Run("test parallel to sequential", func(t *testing.T) {
		src := &errStream{err: errors.New(""), parallel: true}
		dest := src.Sequential()
		at.Error(dest.Err())
		at.False(dest.IsParallel())
		at.NotSame(src, dest)
	})
}

func Test_errStream_Parallel(t *testing.T) {
	at := assert.New(t)

	t.Run("test sequential to parallel", func(t *testing.T) {
		src := &errStream{err: errors.New(""), parallel: false}
		dest := src.Parallel()
		at.Error(dest.Err())
		at.True(dest.IsParallel())
		at.NotSame(src, dest)
	})

	t.Run("test parallel to parallel", func(t *testing.T) {
		src := &errStream{err: errors.New(""), parallel: true}
		dest := src.Parallel()
		at.Error(dest.Err())
		at.True(dest.IsParallel())
		at.Same(src, dest)
	})

}

func Test_errStream_MapToInt(t *testing.T) {
	at := assert.New(t)
	t.Run("test sequential", func(t *testing.T) {
		s := (&errStream{err: errors.New("")}).MapToInt(func(src interface{}) (dest int) {
			return 1
		})
		res, err := s.Collect()
		at.Error(err)
		at.Empty(res)
		at.False(s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := (&errStream{err: errors.New(""), parallel: true}).MapToInt(func(src interface{}) (dest int) {
			return 1
		})
		res, err := s.Collect()
		at.Error(err)
		at.Empty(res)
		at.True(s.IsParallel())
	})
}

func Test_parallelStream_IsParallel(t *testing.T) {
	assert.True(t, (&parallelStream{}).IsParallel())
}

func Test_parallelStream_Collect(t *testing.T) {
	testStreamCollect(t, newParallelStreamForTest)
}

func Test_parallelStream_Distinct(t *testing.T) {
	testStreamDistinct(t, newParallelStreamForTest)
}

func Test_parallelStream_Err(t *testing.T) {
	testStreamErr(t, newParallelStreamForTest([]*element{}))
}

func Test_parallelStream_Filter(t *testing.T) {
	testStreamFilter(t, newParallelStreamForTest)
}

func Test_parallelStream_FlatMap(t *testing.T) {
	testStreamFlatMap(t, newParallelStreamForTest)
}

func Test_parallelStream_Limit(t *testing.T) {
	testStreamLimit(t, newParallelStreamForTest)
}

func Test_parallelStream_Map(t *testing.T) {
	tests := []struct {
		name     string
		elements []int
		expect   []int
	}{
		{
			name:     "test no element",
			elements: []int{},
			expect:   []int{},
		},
		{
			name:     "test 1 element",
			elements: []int{1},
			expect:   []int{2},
		},
		{
			name:     "test 2 element",
			elements: []int{1, 2},
			expect:   []int{2, 4},
		},
		{
			name:     "test 3 element",
			elements: []int{1, 2, 3},
			expect:   []int{2, 4, 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &parallelStream{intSliceToElements(tt.elements)}
			var dest []int
			err := s.Map(func(src interface{}) (dest interface{}) {
				return src.(int) * 2
			}).Collect(&dest)
			assert.NoError(t, err)
			assertSliceEquals(t, tt.expect, dest)
		})
	}
}

func Test_parallelStream_MapToInt(t *testing.T) {
	testStreamMapToInt(t, newParallelStreamForTest)
}

func Test_parallelStream_Reduce(t *testing.T) {
	testStreamReduce(t, newParallelStreamForTest)
}

func Test_parallelStream_Sorted(t *testing.T) {
	testStreamSorted(t, newParallelStreamForTest)
}

func Test_parallelStream_Skip(t *testing.T) {
	testStreamSkip(t, newParallelStreamForTest)
}

func Test_parallelStream_Sequential(t *testing.T) {
	testStreamSequential(t, newParallelStreamForTest)
}

func Test_parallelStream_Parallel(t *testing.T) {
	testStreamParallel(t, newParallelStreamForTest)
}

func testStreamCollect(t *testing.T, stream func([]*element) Stream) {
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
			var result []int
			err := stream(intSliceToElements(tt.elements)).Collect(&result)
			assert.NoError(t, err)
			assertSliceEquals(t, tt.expect, result)
		})
	}

	errTest := []struct {
		name      string
		collector interface{}
	}{
		{
			name:      "test collect into non-slice ptr",
			collector: intPtr(0),
		},
		{
			name:      "test collect into non-ptr",
			collector: []int{},
		},
		{
			name:      "test collect into incorrect element type slice ptr",
			collector: &[]struct{}{},
		},
	}
	for _, tt := range errTest {
		t.Run(tt.name, func(t *testing.T) {
			assert.Error(t, stream(intSliceToElements([]int{1, 2, 3})).Collect(tt.collector))
		})
	}
}

func testStreamDistinct(t *testing.T, stream func([]*element) Stream) {
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
				s := stream(intSliceToElements(tCase.elements))
				parallel := s.IsParallel()
				s = s.Distinct(func(obj interface{}) int {
					return obj.(int)
				}, func(a, b interface{}) bool {
					return a == b
				})
				assert.Equal(t, parallel, s.IsParallel())
				var dest []int
				assert.NoError(t, s.Collect(&dest))
				assertSliceEquals(t, tCase.expect, dest)
			}
		})
	}
}

func testStreamErr(t *testing.T, stream Stream) {
	assert.NoError(t, stream.Err())
}

func testStreamFilter(t *testing.T, stream func([]*element) Stream) {
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
				s := stream(intSliceToElements(tCase.elements))
				parallel := s.IsParallel()
				s = s.Filter(func(val interface{}) (match bool) {
					return val == 1
				})
				assert.Equal(t, parallel, s.IsParallel())
				var dest []int
				assert.NoError(t, s.Collect(&dest))
				assertSliceEquals(t, tCase.expect, dest)
			}
		})
	}
}

func testStreamFlatMap(t *testing.T, stream func([]*element) Stream) {
	at := assert.New(t)
	t.Run("test no elements", func(t *testing.T) {
		var dest []int
		s := stream([]*element{})
		parallel := s.IsParallel()
		s = s.FlatMap(func(val interface{}) Stream {
			return &errStream{err: errors.New("")}
		})
		assert.Equal(t, parallel, s.IsParallel())
		at.NoError(s.Collect(&dest))
		var expect []int
		assertSliceEquals(t, expect, dest)
	})
	t.Run("test FlatMap sequentialStream normal", func(t *testing.T) {
		var dest []int
		s := stream(intSliceToElements([]int{1, 2, 3}))
		parallel := s.IsParallel()
		s = s.FlatMap(func(val interface{}) Stream {
			return newSequentialStreamForTest(intSliceToElements([]int{val.(int), -val.(int)}))
		})
		assert.Equal(t, parallel, s.IsParallel())
		at.NoError(s.Collect(&dest))
		expect := []int{1, -1, 2, -2, 3, -3}
		assertSliceEquals(t, expect, dest)
	})
	t.Run("test FlatMap parallelStream normal", func(t *testing.T) {
		var dest []int
		s := stream(intSliceToElements([]int{1, 2, 3}))
		parallel := s.IsParallel()
		s = s.FlatMap(func(val interface{}) Stream {
			return newParallelStreamForTest(intSliceToElements([]int{val.(int), -val.(int)}))
		})
		assert.Equal(t, parallel, s.IsParallel())
		at.NoError(s.Collect(&dest))
		expect := []int{1, -1, 2, -2, 3, -3}
		assertSliceEquals(t, expect, dest)
	})
	t.Run("test error stream", func(t *testing.T) {
		var dest []int
		s := stream(intSliceToElements([]int{1, 2, 3}))
		parallel := s.IsParallel()
		s = s.FlatMap(func(val interface{}) Stream { return &errStream{err: errors.New("")} })
		assert.Equal(t, parallel, s.IsParallel())
		at.Error(s.Collect(&dest))
	})
}

func testStreamLimit(t *testing.T, stream func([]*element) Stream) {
	t.Run("test maxSize is negative", func(t *testing.T) {
		s := stream([]*element{})
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
				s := stream(intSliceToElements(tCase.elements))
				parallel := s.IsParallel()
				s = s.Limit(tCase.maxSize)
				assert.Equal(t, parallel, s.IsParallel())
				var dest []int
				err := s.Collect(&dest)
				assert.NoError(t, err)
				assertSliceEquals(t, tCase.expect, dest)
			}
		})
	}
}

func testStreamMap(t *testing.T, stream func([]*element) Stream) {
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
		s := stream(intSliceToElements(tt.elements))
		parallel := s.IsParallel()
		s = s.Map(func(src interface{}) (dest interface{}) {
			return src.(int) * 2
		})
		assert.Equal(t, parallel, s.IsParallel())
		var result []int
		err := s.Collect(&result)
		assert.NoError(t, err)
		assertSliceEquals(t, tt.expect, result)
	}
}

func testStreamMapToInt(t *testing.T, stream func([]*element) Stream) {
	tests := [][]int{
		{},
		{1},
		{1, 2},
		{1, 2, 3},
	}
	for _, tt := range tests {
		s := stream(intSliceToElements(tt))
		parallel := s.IsParallel()
		newS := s.MapToInt(func(src interface{}) (dest int) {
			return src.(int)
		})
		assert.Equal(t, parallel, newS.IsParallel())
		dest, err := newS.Collect()
		assert.NoError(t, err)
		assertSliceEquals(t, tt, dest)
	}
}

func testStreamReduce(t *testing.T, stream func([]*element) Stream) {
	tests := []struct {
		elements []int
		expect   interface{}
	}{
		{[]int{}, nil},
		{[]int{1}, 1},
		{[]int{1, 2}, 3},
		{[]int{1, 2, 3}, 6},
		{[]int{1, 2, 3, 4}, 10},
		{[]int{1, 2, 3, 4, 5}, 15},
		{[]int{1, 2, 3, 4, 5, 6}, 21},
		{[]int{1, 2, 3, 4, 5, 6, 7}, 28},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, 36},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8, 9}, 45},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 55},
	}
	for _, tt := range tests {
		result, err := stream(intSliceToElements(tt.elements)).Reduce(func(a, b interface{}) (c interface{}) {
			return a.(int) + b.(int)
		})
		assert.NoError(t, err)
		if tt.expect == nil {
			assert.Nil(t, result)
		} else {
			assert.Equal(t, tt.expect, result)
		}
	}
}

func testStreamSorted(t *testing.T, stream func([]*element) Stream) {
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
				s := stream(intSliceToElements(tCase.elements))
				parallel := s.IsParallel()
				s = s.Sorted(func(a, b interface{}) bool {
					return a.(int) < b.(int)
				})
				assert.Equal(t, parallel, s.IsParallel())
				var res []int
				err := s.Collect(&res)
				at.NoError(err)
				assertSliceEquals(t, tCase.expect, res)
			}
		})
	}
}

func testStreamSkip(t *testing.T, stream func([]*element) Stream) {
	t.Run("test n is negative", func(t *testing.T) {
		assert.Error(t, stream([]*element{}).Skip(-1).Err())
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
			s := stream(intSliceToElements(tt.elements))
			parallel := s.IsParallel()
			s = s.Skip(tt.n)
			assert.Equal(t, parallel, s.IsParallel())
			var res []int
			err := s.Collect(&res)
			assert.NoError(t, err)
			assertSliceEquals(t, tt.expect, res)
		})
	}
}

func testStreamSequential(t *testing.T, stream func([]*element) Stream) {
	assert.False(t, stream([]*element{}).Sequential().IsParallel())
	assert.False(t, stream(intSliceToElements([]int{1})).Sequential().IsParallel())
}

func testStreamParallel(t *testing.T, stream func([]*element) Stream) {
	assert.True(t, stream([]*element{}).Parallel().IsParallel())
}

func intSliceToElements(src []int) (elements []*element) {
	for _, i := range src {
		elements = append(elements, &element{data: i, reflectValue: reflect.ValueOf(i)})
	}
	return elements
}

func newSequentialStreamForTest(elements []*element) Stream {
	return &sequentialStream{elements}
}

func newParallelStreamForTest(elements []*element) Stream {
	return &parallelStream{elements}
}

func assertSliceEquals(t *testing.T, expect, actual []int) {
	equals := func(a, b []int) bool {
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

func TestNewSequentialStream(t *testing.T) {
	t.Run("test empty slice", func(t *testing.T) {
		elements := make([]int, 0)
		s := NewSequentialStream(elements)
		assert.NoError(t, s.Err())
		assert.False(t, s.IsParallel())
	})

	t.Run("test nil slice", func(t *testing.T) {
		var elements []int = nil
		s := NewSequentialStream(elements)
		assert.NoError(t, s.Err())
		assert.False(t, s.IsParallel())
	})

	t.Run("test slice", func(t *testing.T) {
		stream := NewSequentialStream([]int{1, 2, 3})
		assert.NoError(t, stream.Err())
		assert.False(t, stream.IsParallel())
	})
	t.Run("test not slice", func(t *testing.T) {
		s := NewSequentialStream(&[]int{1, 2, 3})
		assert.Error(t, s.Err())
		assert.False(t, s.IsParallel())
	})
}

func TestNewParallelStream(t *testing.T) {
	t.Run("test empty slice", func(t *testing.T) {
		elements := make([]int, 0)
		s := NewParallelStream(elements)
		assert.NoError(t, s.Err())
		assert.True(t, s.IsParallel())
	})

	t.Run("test nil slice", func(t *testing.T) {
		var elements []int = nil
		s := NewParallelStream(elements)
		assert.NoError(t, s.Err())
		assert.True(t, s.IsParallel())
	})
	t.Run("test empty slice", func(t *testing.T) {
		var emptySlice []int
		s := NewParallelStream(emptySlice)
		assert.NoError(t, s.Err())
		assert.True(t, s.IsParallel())
	})
	t.Run("test slice", func(t *testing.T) {
		s := NewParallelStream([]int{1, 2, 3})
		assert.NoError(t, s.Err())
		assert.True(t, s.IsParallel())
	})
	t.Run("test not slice", func(t *testing.T) {
		s := NewParallelStream(&[]int{1, 2, 3})
		assert.Error(t, s.Err())
		assert.True(t, s.IsParallel())
	})
}

func Test_sequentialStream_MapToF64(t *testing.T) {
	testStreamMapToF64(t, newSequentialStreamForTest)
}

func Test_parallelStream_MapToF64(t *testing.T) {
	testStreamMapToF64(t, newParallelStreamForTest)
}

func Test_errStream_MapToF64(t *testing.T) {
	t.Run("test sequential", func(t *testing.T) {
		s := (&errStream{err: errors.New("")}).MapToFloat64(func(src interface{}) (dest float64) {
			return 1
		})
		res, err := s.Collect()
		assert.Error(t, err)
		assert.Empty(t, res)
		assert.False(t, s.IsParallel())
	})
	t.Run("test parallel", func(t *testing.T) {
		s := (&errStream{err: errors.New(""), parallel: true}).MapToFloat64(func(src interface{}) (dest float64) {
			return 1
		})
		res, err := s.Collect()
		assert.Error(t, err)
		assert.Empty(t, res)
		assert.True(t, s.IsParallel())
	})
}

func testStreamMapToF64(t *testing.T, stream func([]*element) Stream) {
	tests := []struct {
		elements []int
		expect   []float64
	}{
		{nil, nil},
		{[]int{1}, []float64{1}},
		{[]int{1, 2}, []float64{1, 2}},
		{[]int{1, 2, 3}, []float64{1, 2, 3}},
	}
	for _, tt := range tests {
		s := stream(intSliceToElements(tt.elements))
		parallel := s.IsParallel()
		newS := s.MapToFloat64(func(src interface{}) (dest float64) {
			return float64(src.(int))
		})
		assert.Equal(t, parallel, newS.IsParallel())
		dest, err := newS.Collect()
		assert.NoError(t, err)
		assertFloat64SliceEquals(t, tt.expect, dest)
	}
}
