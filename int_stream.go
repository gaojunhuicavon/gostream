package gostream

import (
	"fmt"
	"sort"
	"sync"
)

var (
	emptySequentialIntStream = &sequentialIntStream{}
	emptyParallelIntStream   = &parallelIntStream{}
)

// IntStream is a sequence of int-valued elements supporting sequential and parallel aggregate operations.
type IntStream interface {
	BaseStream
	// Average returns a *float64 describing the arithmetic mean of elements of this stream, or a nil pointer
	// if this stream is empty.
	Average() (*float64, error)
	// Collect returns a []int consisting of the elements of this stream.
	Collect() ([]int, error)
	// Distinct returns a stream consisting of the distinct elements of this stream.
	Distinct() IntStream
	// Filter returns a stream consisting of the results of applying the given mapper to the elements of this stream.
	Filter(predicate func(val int) (match bool)) IntStream
	// FlatMap returns a stream consisting of the results of replacing each element of this stream with the contents
	// of a mapped stream produced by applying the provided mapper to each element.
	FlatMap(mapper func(val int) IntStream) IntStream
	// Limit returns a stream consisting of the elements of this stream, truncated to be no longer than maxSize in
	// length.
	// An error will occur when maxSize is negative.
	Limit(maxSize int) IntStream
	// Map returns a stream consisting of results of applying the given mapper to the elements of this stream.
	Map(mapper func(src int) (dest int)) IntStream
	// MapToFloat64 returns a Float64Stream consisting of the results of applying the given mapper to the elements of
	// this stream.
	MapToFloat64(mapper func(src int) (dest float64)) Float64Stream
	// MapToObj returns an object-valued Stream consisting of the results of applying the given mapper to the elements
	// of this stream.
	MapToObj(mapper func(src int) (dest interface{})) Stream
	// Max returns a *int describing the maximum element of this stream, or a nil pointer if the stream is empty.
	Max() (*int, error)
	// Min returns a *int describing the minimum element of this stream, or a nil pointer if the stream is empty.
	Min() (*int, error)
	// Parallel returns an equivalent stream that is parallel. May return itself, because the stream was already
	// parallel.
	Parallel() IntStream
	// Reduce performs a reduction on the elements of this stream, using an associative accumulation function, and
	// returns a *int describing the reduced value, if any, or a nil pointer if the stream is empty.
	Reduce(op func(a, b int) (c int)) (*int, error)
	// Sequential returns an equivalent stream that is sequential. May return itself, because the stream was already
	// sequential.
	Sequential() IntStream
	// Skip returns a stream consisting of the remaining elements of this stream after discarding the first n elements
	// of the stream. If this stream contains fewer than n elements then an empty stream will be returned.
	// An error will occur if n is negative.
	Skip(n int) IntStream
	// Sorted returns a stream consisting of the elements of this stream in sorted order.
	Sorted() IntStream
}

type sequentialIntStream struct {
	elements []int
}

type parallelIntStream struct {
	elements []int
}

type errIntStream struct {
	err      error
	parallel bool
}

func NewSequentialIntStream(ints []int) IntStream {
	return &sequentialIntStream{ints}
}

func NewParallelIntStream(ints []int) IntStream {
	return &parallelIntStream{ints}
}

func ConcatIntStream(a, b IntStream) IntStream {
	aElements, err := a.Collect()
	if err != nil {
		return &errIntStream{err: err}
	}
	bElements, err := b.Collect()
	if err != nil {
		return &errIntStream{err: err}
	}
	return &sequentialIntStream{append(aElements, bElements...)}
}

func (s *sequentialIntStream) MapToObj(mapper func(src int) (dest interface{})) Stream {
	if len(s.elements) <= 0 {
		return emptySequentialStream
	}
	dest := make([]interface{}, 0, len(s.elements))
	for _, e := range s.elements {
		dest = append(dest, mapper(e))
	}
	return NewSequentialStream(dest)
}

func (s *sequentialIntStream) Err() error {
	return nil
}

func (s *sequentialIntStream) IsParallel() bool {
	return false
}

func (s *sequentialIntStream) Average() (*float64, error) {
	if len(s.elements) == 0 {
		return nil, nil
	}
	sum := 0
	for _, e := range s.elements {
		sum += e
	}
	result := float64(sum) / float64(len(s.elements))
	return &result, nil
}

func (s *sequentialIntStream) Collect() ([]int, error) {
	result := make([]int, len(s.elements))
	copy(result, s.elements)
	return result, nil
}

func (s *sequentialIntStream) Distinct() IntStream {
	if len(s.elements) <= 1 {
		return s
	}
	seen := map[int]bool{}
	result := make([]int, 0)
	for _, e := range s.elements {
		if seen[e] {
			continue
		}
		seen[e] = true
		result = append(result, e)
	}
	return &sequentialIntStream{elements: result}
}

func (s *sequentialIntStream) Filter(predicate func(val int) (keep bool)) IntStream {
	if len(s.elements) == 0 {
		return s
	}
	result := make([]int, 0)
	for _, e := range s.elements {
		if predicate(e) {
			result = append(result, e)
		}
	}
	return &sequentialIntStream{elements: result}
}

func (s *sequentialIntStream) FlatMap(mapper func(val int) IntStream) IntStream {
	if len(s.elements) == 0 {
		return s
	}
	newElements := make([]int, 0)
	for _, e := range s.elements {
		ints, err := mapper(e).Collect()
		if err != nil {
			return &errIntStream{err: err}
		}
		newElements = append(newElements, ints...)
	}
	return &sequentialIntStream{elements: newElements}
}

func (s *sequentialIntStream) Limit(maxSize int) IntStream {
	if maxSize < 0 {
		return &errIntStream{err: fmt.Errorf("limit error, maxSize less than 0: %d", maxSize)}
	}
	if maxSize == 0 {
		return emptySequentialIntStream
	}
	if maxSize >= len(s.elements) {
		return s
	}
	return &sequentialIntStream{elements: s.elements[:maxSize]}
}

func (s *sequentialIntStream) Map(mapper func(src int) (dest int)) IntStream {
	if len(s.elements) == 0 {
		return s
	}
	newElements := make([]int, 0, len(s.elements))
	for _, e := range s.elements {
		newElements = append(newElements, mapper(e))
	}
	return &sequentialIntStream{elements: newElements}
}

func (s *sequentialIntStream) Max() (*int, error) {
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

func (s *sequentialIntStream) Min() (*int, error) {
	if len(s.elements) == 0 {
		return nil, nil
	}
	result := s.elements[0]
	for i := 1; i < len(s.elements); i++ {
		if e := s.elements[i]; e < result {
			result = e
		}
	}
	return &result, nil
}

func (s *sequentialIntStream) Parallel() IntStream {
	if len(s.elements) <= 0 {
		return emptyParallelIntStream
	}
	return NewParallelIntStream(s.elements)
}

func (s *sequentialIntStream) Reduce(op func(a, b int) (c int)) (*int, error) {
	if len(s.elements) == 0 {
		return nil, nil
	}
	result := s.elements[0]
	for i := 1; i < len(s.elements); i++ {
		result = op(result, s.elements[i])
	}
	return &result, nil
}

func (s *sequentialIntStream) Sequential() IntStream {
	return s
}

func (s *sequentialIntStream) Skip(n int) IntStream {
	if n < 0 {
		return &errIntStream{err: fmt.Errorf("skip error, skipN less than 0: %d", n)}
	}
	if n == 0 {
		return s
	}
	if n >= len(s.elements) {
		return emptySequentialIntStream
	}
	return &sequentialIntStream{elements: s.elements[n:]}
}

func (s *sequentialIntStream) Sorted() IntStream {
	if sort.IntsAreSorted(s.elements) {
		return s
	}
	newElements := make([]int, len(s.elements))
	copy(newElements, s.elements)
	sort.Sort(sort.IntSlice(newElements))
	return &sequentialIntStream{elements: newElements}
}

func (s *sequentialIntStream) MapToFloat64(mapper func(src int) (dest float64)) Float64Stream {
	if len(s.elements) == 0 {
		return emptySequentialFloat64Stream
	}
	newElements := make([]float64, 0, len(s.elements))
	for _, e := range s.elements {
		newElements = append(newElements, mapper(e))
	}
	return NewSequentialFloat64Stream(newElements)
}

func (p *parallelIntStream) IsParallel() bool {
	return true
}

func (p *parallelIntStream) Average() (*float64, error) {
	if len(p.elements) == 0 {
		return nil, nil
	}
	sum := p.elements[0]
	for i := 1; i < len(p.elements); i++ {
		sum += p.elements[i]
	}
	avg := float64(sum) / float64(len(p.elements))
	return &avg, nil
}

func (p *parallelIntStream) Collect() ([]int, error) {
	res := make([]int, len(p.elements))
	copy(res, p.elements)
	return res, nil
}

func (p *parallelIntStream) Distinct() IntStream {
	if len(p.elements) <= 1 {
		return p
	}
	seen := make(map[int]bool)
	remain := make([]int, 0)
	for _, e := range p.elements {
		if !seen[e] {
			seen[e] = true
			remain = append(remain, e)
		}
	}
	return &parallelIntStream{remain}
}

func (p *parallelIntStream) Err() error {
	return nil
}

func (p *parallelIntStream) Filter(predicate func(val int) (keep bool)) IntStream {
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

	remain := make([]int, 0)
	for i, m := range match {
		if m {
			remain = append(remain, p.elements[i])
		}
	}
	return &parallelIntStream{remain}
}

func (p *parallelIntStream) FlatMap(mapper func(val int) IntStream) IntStream {
	if len(p.elements) == 0 {
		return p
	}
	streams := make([]IntStream, len(p.elements))
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

	newElements := make([]int, 0)
	for _, stream := range streams {
		elements, err := stream.Collect()
		if err != nil {
			return &errIntStream{err: err, parallel: true}
		}
		newElements = append(newElements, elements...)
	}
	return &parallelIntStream{newElements}
}

func (p *parallelIntStream) Limit(maxSize int) IntStream {
	if maxSize < 0 {
		return &errIntStream{err: fmt.Errorf("limit error, maxSize is negative: %d", maxSize), parallel: true}
	}
	if maxSize == 0 {
		return emptyParallelIntStream
	}
	if maxSize >= len(p.elements) {
		return p
	}
	return &parallelIntStream{p.elements[:maxSize]}
}

func (p *parallelIntStream) Map(mapper func(src int) (dest int)) IntStream {
	if len(p.elements) == 0 {
		return p
	}
	newElements := make([]int, len(p.elements))
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
	return &parallelIntStream{newElements}
}

func (p *parallelIntStream) MapToObj(mapper func(src int) (dest interface{})) Stream {
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

func (p *parallelIntStream) Max() (*int, error) {
	if len(p.elements) == 0 {
		return nil, nil
	}
	max := p.elements[0]
	for i := 1; i < len(p.elements); i++ {
		if p.elements[i] > max {
			max = p.elements[i]
		}
	}
	return &max, nil
}

func (p *parallelIntStream) Min() (*int, error) {
	if len(p.elements) == 0 {
		return nil, nil
	}
	min := p.elements[0]
	for i := 1; i < len(p.elements); i++ {
		if p.elements[i] < min {
			min = p.elements[i]
		}
	}
	return &min, nil
}

func (p *parallelIntStream) Parallel() IntStream {
	return p
}

func (p *parallelIntStream) Reduce(op func(a, b int) (c int)) (*int, error) {
	if len(p.elements) == 0 {
		return nil, nil
	}
	task := newIntReduceRecursiveTask(op, p.elements, 0, len(p.elements)-1)
	result := task.compute().(int)
	return &result, nil
}

func (p *parallelIntStream) Sequential() IntStream {
	if len(p.elements) <= 0 {
		return emptySequentialIntStream
	}
	return &sequentialIntStream{p.elements}
}

func (p *parallelIntStream) Skip(n int) IntStream {
	if n < 0 {
		return &errIntStream{err: fmt.Errorf("skip error, n is negative: %v", n), parallel: true}
	}
	if n == 0 {
		return p
	}
	if n >= len(p.elements) {
		return emptyParallelIntStream
	}
	return &parallelIntStream{p.elements[n:]}
}

func (p *parallelIntStream) Sorted() IntStream {
	if len(p.elements) <= 1 || sort.IntsAreSorted(p.elements) {
		return p
	}
	newElements := make([]int, len(p.elements))
	copy(newElements, p.elements)
	sort.Ints(newElements)
	return &parallelIntStream{newElements}
}

func (p *parallelIntStream) MapToFloat64(mapper func(src int) (dest float64)) Float64Stream {
	if len(p.elements) == 0 {
		return emptyParallelFloat64Stream
	}
	objs := make([]float64, len(p.elements))
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
	return NewParallelFloat64Stream(objs)

}

func (e *errIntStream) MapToObj(func(src int) (dest interface{})) Stream {
	return &errStream{err: e.err, parallel: e.parallel}
}

func (e *errIntStream) Err() error {
	return e.err
}

func (e *errIntStream) IsParallel() bool {
	return e.parallel
}

func (e *errIntStream) Average() (*float64, error) {
	return nil, e.err
}

func (e *errIntStream) Collect() ([]int, error) {
	return nil, e.err
}

func (e *errIntStream) Distinct() IntStream {
	return e
}

func (e *errIntStream) Filter(func(val int) (keep bool)) IntStream {
	return e
}

func (e *errIntStream) FlatMap(func(val int) IntStream) IntStream {
	return e
}

func (e *errIntStream) Limit(int) IntStream {
	return e
}

func (e *errIntStream) Map(func(src int) (dest int)) IntStream {
	return e
}

func (e *errIntStream) MapToFloat64(func(src int) (dest float64)) Float64Stream {
	return &errFloat64Stream{err: e.err, parallel: e.parallel}
}

func (e *errIntStream) Max() (*int, error) {
	return nil, e.err
}

func (e *errIntStream) Min() (*int, error) {
	return nil, e.err
}

func (e *errIntStream) Parallel() IntStream {
	if e.parallel {
		return e
	}
	return &errIntStream{err: e.err, parallel: true}
}

func (e *errIntStream) Reduce(func(a, b int) (c int)) (*int, error) {
	return nil, e.err
}

func (e *errIntStream) Sequential() IntStream {
	if e.parallel {
		return &errIntStream{err: e.err, parallel: false}
	}
	return e
}

func (e *errIntStream) Skip(int) IntStream {
	return e
}

func (e *errIntStream) Sorted() IntStream {
	return e
}
