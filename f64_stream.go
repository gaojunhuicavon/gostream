package gostream

import (
	"fmt"
	"sort"
	"sync"
)

var (
	emptySequentialFloat64Stream = &sequentialFloat64Stream{}
	emptyParallelFloat64Stream   = &parallelFloat64Stream{}
)

// Float64Stream is a sequence of float64-valued elements supporting sequential and parallel aggregate operations.
type Float64Stream interface {
	BaseStream
	// Average returns a *float64 describing the arithmetic mean of elements of this stream, or a nil pointer
	// if this stream is empty.
	Average() (*float64, error)
	// Collect returns a []float64 consisting of the elements of this stream.
	Collect() ([]float64, error)
	// Distinct returns a stream consisting of the distinct elements of this stream.
	Distinct() Float64Stream
	// Filter returns a stream consisting of the results of applying the given mapper to the elements of this stream.
	Filter(predicate func(val float64) (match bool)) Float64Stream
	// FlatMap returns a stream consisting of the results of replacing each element of this stream with the contents
	// of a mapped stream produced by applying the provided mapper to each element.
	FlatMap(mapper func(val float64) Float64Stream) Float64Stream
	// Limit returns a stream consisting of the elements of this stream, truncated to be no longer than maxSize in
	// length.
	// An error will occur when maxSize is negative.
	Limit(maxSize int) Float64Stream
	// Map returns a stream consisting of results of applying the given mapper to the elements of this stream.
	Map(mapper func(src float64) (dest float64)) Float64Stream
	// MapToInt returns an IntStream consisting of the results of applying the given mapper to the elements of
	// this stream.
	MapToInt(mapper func(src float64) (dest int)) IntStream
	// MapToObj returns an object-valued Stream consisting of the results of applying the given mapper to the elements
	// of this stream.
	MapToObj(mapper func(src float64) (dest interface{})) Stream
	// Max returns a *int describing the maximum element of this stream, or a nil pointer if the stream is empty.
	Max() (*float64, error)
	// Min returns a *int describing the minimum element of this stream, or a nil pointer if the stream is empty.
	Min() (*float64, error)
	// Parallel returns an equivalent stream that is parallel. May return itself, because the stream was already
	// parallel.
	Parallel() Float64Stream
	// Reduce performs a reduction on the elements of this stream, using an associative accumulation function, and
	// returns a *int describing the reduced value, if any, or a nil pointer if the stream is empty.
	Reduce(op func(a, b float64) (c float64)) (*float64, error)
	// Sequential returns an equivalent stream that is sequential. May return itself, because the stream was already
	// sequential.
	Sequential() Float64Stream
	// Skip returns a stream consisting of the remaining elements of this stream after discarding the first n elements
	// of the stream. If this stream contains fewer than n elements then an empty stream will be returned.
	// An error will occur if n is negative.
	Skip(n int) Float64Stream
	// Sorted returns a stream consisting of the elements of this stream in sorted order.
	Sorted() Float64Stream
	// Returns the sum of elements in this stream.
	Sum() (float64, error)
}

type sequentialFloat64Stream struct {
	elements []float64
}

type parallelFloat64Stream struct {
	elements []float64
}

type errFloat64Stream struct {
	err      error
	parallel bool
}

func NewSequentialFloat64Stream(data []float64) Float64Stream {
	if len(data) <= 0 {
		return emptySequentialFloat64Stream
	}
	return &sequentialFloat64Stream{data}
}

func NewParallelFloat64Stream(data []float64) Float64Stream {
	if len(data) <= 0 {
		return emptyParallelFloat64Stream
	}
	return &parallelFloat64Stream{data}
}

func (e *errFloat64Stream) IsParallel() bool {
	return e.parallel
}

func (e *errFloat64Stream) Average() (*float64, error) {
	return nil, e.err
}

func (e *errFloat64Stream) Collect() ([]float64, error) {
	return nil, e.err
}

func (e *errFloat64Stream) Distinct() Float64Stream {
	return e
}

func (e *errFloat64Stream) Filter(func(val float64) (match bool)) Float64Stream {
	return e
}

func (e *errFloat64Stream) FlatMap(func(val float64) Float64Stream) Float64Stream {
	return e
}

func (e *errFloat64Stream) Limit(int) Float64Stream {
	return e
}

func (e *errFloat64Stream) Map(func(src float64) (dest float64)) Float64Stream {
	return e
}

func (e *errFloat64Stream) MapToInt(func(src float64) (dest int)) IntStream {
	return &errIntStream{err: e.err, parallel: e.parallel}
}

func (e *errFloat64Stream) MapToObj(func(src float64) (dest interface{})) Stream {
	return &errStream{err: e.err, parallel: e.parallel}
}

func (e *errFloat64Stream) Max() (*float64, error) {
	return nil, e.err
}

func (e *errFloat64Stream) Min() (*float64, error) {
	return nil, e.err
}

func (e *errFloat64Stream) Parallel() Float64Stream {
	if e.parallel {
		return e
	}
	return &errFloat64Stream{
		err:      e.err,
		parallel: true,
	}
}

func (e *errFloat64Stream) Reduce(func(a, b float64) (c float64)) (*float64, error) {
	return nil, e.err
}

func (e *errFloat64Stream) Sequential() Float64Stream {
	if e.parallel {
		return &errFloat64Stream{
			err:      e.err,
			parallel: false,
		}
	}
	return e
}

func (e *errFloat64Stream) Skip(int) Float64Stream {
	return e
}

func (e *errFloat64Stream) Sorted() Float64Stream {
	return e
}

func (e *errFloat64Stream) Sum() (float64, error) {
	return 0, e.err
}

func (e *errFloat64Stream) Err() error {
	return e.err
}

func (p *parallelFloat64Stream) IsParallel() bool {
	return true
}

func (p *parallelFloat64Stream) Average() (*float64, error) {
	if len(p.elements) <= 0 {
		return nil, nil
	}
	var sum float64 = 0
	for _, e := range p.elements {
		sum += e
	}
	result := sum / float64(len(p.elements))
	return &result, nil
}

func (p *parallelFloat64Stream) Collect() ([]float64, error) {
	result := make([]float64, len(p.elements))
	copy(result, p.elements)
	return result, nil
}

func (p *parallelFloat64Stream) Distinct() Float64Stream {
	if len(p.elements) <= 1 {
		return p
	}
	seen := make(map[float64]bool)
	remain := make([]float64, 0)
	for _, e := range p.elements {
		if seen[e] {
			continue
		}
		seen[e] = true
		remain = append(remain, e)
	}
	return &parallelFloat64Stream{remain}
}

func (p *parallelFloat64Stream) Filter(predicate func(val float64) (match bool)) Float64Stream {
	if len(p.elements) == 0 {
		return p
	}
	match := make([]bool, len(p.elements))
	var wg sync.WaitGroup
	wg.Add(len(p.elements) - 1)
	for i := 1; i < len(p.elements); i++ {
		index := i
		go func() {
			defer wg.Done()
			match[index] = predicate(p.elements[index])
		}()
	}
	match[0] = predicate(p.elements[0])
	wg.Wait()

	remain := make([]float64, 0)
	for i, m := range match {
		if m {
			remain = append(remain, p.elements[i])
		}
	}
	return &parallelFloat64Stream{remain}
}

func (p *parallelFloat64Stream) FlatMap(mapper func(val float64) Float64Stream) Float64Stream {
	if len(p.elements) == 0 {
		return p
	}
	streams := make([]Float64Stream, len(p.elements))
	var wg sync.WaitGroup
	wg.Add(len(p.elements) - 1)
	for i := 1; i < len(p.elements); i++ {
		index := i
		go func() {
			defer wg.Done()
			streams[index] = mapper(p.elements[index])
		}()
	}
	streams[0] = mapper(p.elements[0])
	wg.Wait()

	newElements := make([]float64, 0)
	for _, stream := range streams {
		elements, err := stream.Collect()
		if err != nil {
			return &errFloat64Stream{err: err, parallel: true}
		}
		newElements = append(newElements, elements...)
	}
	return &parallelFloat64Stream{newElements}

}

func (p *parallelFloat64Stream) Limit(maxSize int) Float64Stream {
	if maxSize < 0 {
		return &errFloat64Stream{err: fmt.Errorf("limit error, maxSize is negative: %d", maxSize), parallel: true}
	}
	if maxSize == 0 {
		return emptyParallelFloat64Stream
	}
	if maxSize >= len(p.elements) {
		return p
	}
	return &parallelFloat64Stream{p.elements[:maxSize]}
}

func (p *parallelFloat64Stream) Map(mapper func(src float64) (dest float64)) Float64Stream {
	if len(p.elements) == 0 {
		return p
	}
	newElements := make([]float64, len(p.elements))
	var wg sync.WaitGroup
	wg.Add(len(p.elements) - 1)
	for i := 1; i < len(p.elements); i++ {
		index := i
		go func() {
			defer wg.Done()
			newElements[index] = mapper(p.elements[index])
		}()
	}
	newElements[0] = mapper(p.elements[0])
	wg.Wait()
	return &parallelFloat64Stream{newElements}
}

func (p *parallelFloat64Stream) MapToInt(mapper func(src float64) (dest int)) IntStream {
	if len(p.elements) == 0 {
		return emptyParallelIntStream
	}
	objs := make([]int, len(p.elements))
	var wg sync.WaitGroup
	wg.Add(len(p.elements) - 1)
	for i := 1; i < len(p.elements); i++ {
		index := i
		go func() {
			defer wg.Done()
			objs[index] = mapper(p.elements[index])
		}()
	}
	objs[0] = mapper(p.elements[0])
	wg.Wait()
	return &parallelIntStream{objs}
}

func (p *parallelFloat64Stream) MapToObj(mapper func(src float64) (dest interface{})) Stream {
	if len(p.elements) == 0 {
		return emptyParallelStream
	}
	objs := make([]interface{}, len(p.elements))
	var wg sync.WaitGroup
	wg.Add(len(p.elements) - 1)
	for i := 1; i < len(p.elements); i++ {
		index := i
		go func() {
			defer wg.Done()
			objs[index] = mapper(p.elements[index])
		}()
	}
	objs[0] = mapper(p.elements[0])
	wg.Wait()
	return NewParallelStream(objs)
}

func (p *parallelFloat64Stream) Max() (*float64, error) {
	if len(p.elements) <= 0 {
		return nil, nil
	}
	max := p.elements[0]
	for _, e := range p.elements {
		if e > max {
			max = e
		}
	}
	return &max, nil
}

func (p *parallelFloat64Stream) Min() (*float64, error) {
	if len(p.elements) <= 0 {
		return nil, nil
	}
	min := p.elements[0]
	for _, e := range p.elements {
		if e < min {
			min = e
		}
	}
	return &min, nil
}

func (p *parallelFloat64Stream) Parallel() Float64Stream {
	return p
}

func (p *parallelFloat64Stream) Reduce(op func(a, b float64) (c float64)) (*float64, error) {
	if len(p.elements) == 0 {
		return nil, nil
	}
	task := newFloat64ReduceRecursiveTask(op, p.elements, 0, len(p.elements)-1)
	result := task.compute().(float64)
	return &result, nil
}

func (p *parallelFloat64Stream) Sequential() Float64Stream {
	if len(p.elements) <= 0 {
		return emptySequentialFloat64Stream
	}
	newElements := make([]float64, len(p.elements))
	copy(newElements, p.elements)
	return &sequentialFloat64Stream{newElements}
}

func (p *parallelFloat64Stream) Skip(n int) Float64Stream {
	if n < 0 {
		return &errFloat64Stream{err: fmt.Errorf("skip error, n is negative: %v", n), parallel: true}
	}
	if n == 0 {
		return p
	}
	if n >= len(p.elements) {
		return emptyParallelFloat64Stream
	}
	return &parallelFloat64Stream{p.elements[n:]}
}

func (p *parallelFloat64Stream) Sorted() Float64Stream {
	if len(p.elements) <= 1 || sort.Float64sAreSorted(p.elements) {
		return p
	}
	newElements := make([]float64, len(p.elements))
	copy(newElements, p.elements)
	sort.Float64s(newElements)
	return &parallelFloat64Stream{newElements}
}

func (p *parallelFloat64Stream) Sum() (float64, error) {
	var sum float64 = 0
	for _, e := range p.elements {
		sum += e
	}
	return sum, nil
}

func (p *parallelFloat64Stream) Err() error {
	return nil
}

func (s *sequentialFloat64Stream) IsParallel() bool {
	return false
}

func (s *sequentialFloat64Stream) Average() (*float64, error) {
	if len(s.elements) == 0 {
		return nil, nil
	}
	var sum float64 = 0
	for _, e := range s.elements {
		sum += e
	}
	result := sum / float64(len(s.elements))
	return &result, nil
}

func (s *sequentialFloat64Stream) Collect() ([]float64, error) {
	result := make([]float64, len(s.elements))
	copy(result, s.elements)
	return result, nil
}

func (s *sequentialFloat64Stream) Distinct() Float64Stream {
	if len(s.elements) <= 1 {
		return s
	}
	seen := map[float64]bool{}
	result := make([]float64, 0)
	for _, e := range s.elements {
		if seen[e] {
			continue
		}
		seen[e] = true
		result = append(result, e)
	}
	return &sequentialFloat64Stream{elements: result}
}

func (s *sequentialFloat64Stream) Filter(predicate func(val float64) (match bool)) Float64Stream {
	if len(s.elements) == 0 {
		return s
	}
	result := make([]float64, 0)
	for _, e := range s.elements {
		if predicate(e) {
			result = append(result, e)
		}
	}
	return &sequentialFloat64Stream{result}
}

func (s *sequentialFloat64Stream) FlatMap(mapper func(val float64) Float64Stream) Float64Stream {
	if len(s.elements) == 0 {
		return s
	}
	newElements := make([]float64, 0)
	for _, e := range s.elements {
		float64s, err := mapper(e).Collect()
		if err != nil {
			return &errFloat64Stream{err: err}
		}
		newElements = append(newElements, float64s...)
	}
	return &sequentialFloat64Stream{newElements}
}

func (s *sequentialFloat64Stream) Limit(maxSize int) Float64Stream {
	if maxSize < 0 {
		return &errFloat64Stream{err: fmt.Errorf("limit error, maxSize less than 0: %d", maxSize)}
	}
	if maxSize == 0 {
		return emptySequentialFloat64Stream
	}
	if maxSize >= len(s.elements) {
		return s
	}
	return &sequentialFloat64Stream{s.elements[:maxSize]}
}

func (s *sequentialFloat64Stream) Map(mapper func(src float64) (dest float64)) Float64Stream {
	if len(s.elements) == 0 {
		return s
	}
	newElements := make([]float64, 0, len(s.elements))
	for _, e := range s.elements {
		newElements = append(newElements, mapper(e))
	}
	return &sequentialFloat64Stream{newElements}
}

func (s *sequentialFloat64Stream) MapToInt(mapper func(src float64) (dest int)) IntStream {
	if len(s.elements) == 0 {
		return emptySequentialIntStream
	}
	newElements := make([]int, 0, len(s.elements))
	for _, e := range s.elements {
		newElements = append(newElements, mapper(e))
	}
	return &sequentialIntStream{elements: newElements}
}

func (s *sequentialFloat64Stream) MapToObj(mapper func(src float64) (dest interface{})) Stream {
	if len(s.elements) <= 0 {
		return emptySequentialStream
	}
	dest := make([]interface{}, 0, len(s.elements))
	for _, e := range s.elements {
		dest = append(dest, mapper(e))
	}
	return NewSequentialStream(dest)
}

func (s *sequentialFloat64Stream) Max() (*float64, error) {
	if len(s.elements) == 0 {
		return nil, nil
	}
	result := s.elements[0]
	for i := 1; i < len(s.elements); i++ {
		if e := s.elements[i]; e > result {
			result = e
		}
	}
	return &result, nil
}

func (s *sequentialFloat64Stream) Min() (*float64, error) {
	if len(s.elements) == 0 {
		return nil, nil
	}
	min := s.elements[0]
	for _, e := range s.elements {
		if e < min {
			min = e
		}
	}
	return &min, nil
}

func (s *sequentialFloat64Stream) Parallel() Float64Stream {
	if len(s.elements) <= 0 {
		return emptyParallelFloat64Stream
	}
	return &parallelFloat64Stream{s.elements}
}

func (s *sequentialFloat64Stream) Reduce(op func(a, b float64) (c float64)) (*float64, error) {
	if len(s.elements) == 0 {
		return nil, nil
	}
	result := s.elements[0]
	for i := 1; i < len(s.elements); i++ {
		result = op(result, s.elements[i])
	}
	return &result, nil

}

func (s *sequentialFloat64Stream) Sequential() Float64Stream {
	return s
}

func (s *sequentialFloat64Stream) Skip(n int) Float64Stream {
	if n < 0 {
		return &errFloat64Stream{err: fmt.Errorf("skip error, skipN less than 0: %d", n)}
	}
	if n == 0 {
		return s
	}
	if n >= len(s.elements) {
		return emptySequentialFloat64Stream
	}
	return &sequentialFloat64Stream{s.elements[n:]}
}

func (s *sequentialFloat64Stream) Sorted() Float64Stream {
	if sort.Float64sAreSorted(s.elements) {
		return s
	}
	newElements := make([]float64, len(s.elements))
	copy(newElements, s.elements)
	sort.Float64s(newElements)
	return &sequentialFloat64Stream{newElements}
}

func (s *sequentialFloat64Stream) Sum() (float64, error) {
	var sum float64 = 0
	for _, e := range s.elements {
		sum += e
	}
	return sum, nil
}

func (s *sequentialFloat64Stream) Err() error {
	return nil
}
