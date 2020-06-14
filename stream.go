package gostream

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

var (
	emptySequentialStream = &sequentialStream{}
	emptyParallelStream   = &parallelStream{}
)

type BaseStream interface {
	// IsParallel returns whether this stream execute in parallel.
	IsParallel() bool
	// Err returns the error occurred in the stream, nil is returned if no error occurred.
	Err() error
}

// Stream is a sequence of elements supporting sequential and parallel aggregate operations.
type Stream interface {
	BaseStream
	// Collect write the elements in the stream to the collector, the collector should be a pointer to Slice
	// than can store the elements in the stream.
	Collect(collector interface{}) error
	// Distinct returns a stream consisting of the distinct elements of this stream.
	// hashcode should return the hashcode of obj, 2 equals object should return the same hashcode.
	// equals should return true if a and b are equal, otherwise should return false.
	Distinct(hashcode func(obj interface{}) int, equals func(a, b interface{}) bool) Stream
	// Filter returns a stream consisting of the elements of this stream that match the given predicate.
	Filter(predicate func(val interface{}) (match bool)) Stream
	// FlatMap returns a stream consisting of the results of replacing each element of this stream with the contents of
	// a mapped stream produced by applying mapper to each element.
	FlatMap(mapper func(val interface{}) Stream) Stream
	// Limit returns a stream consisting of elements of this stream, truncated to be no longer than maxSize in length，
	// An error will occur when maxSize is negative.
	Limit(maxSize int) Stream
	// Map returns a steam consisting of the results of applying mapper to the elements of this stream.
	Map(mapper func(src interface{}) (dest interface{})) Stream
	// MapToFloat64 returns a Float64Stream consisting of the results of applying the given mapper to the elements of
	// this stream.
	MapToFloat64(mapper func(src interface{}) (dest float64)) Float64Stream
	// MapToInt returns an IntStream consisting of the results of applying the given mapper to the elements of
	// this stream.
	MapToInt(mapper func(src interface{}) (dest int)) IntStream
	// Reduce performs a reduction on the elements of this stream, using an associative accumulation function,
	// and return the reduced value if any, otherwise nil will be returned.
	// Reduction won't be performed if the stream contains an error, and the error will be returned.
	Reduce(accumulator func(a, b interface{}) (c interface{})) (interface{}, error)
	// Sorted returns a stream consisting of the elements of this stream, sorted according to less.
	Sorted(less func(a, b interface{}) bool) Stream
	// Skip returns a stream consisting of the remaining elements of this stream after discarding
	// the first n elements of the stream.
	// If this stream contains fewer than n elements then an empty stream will be returned.
	// An error will occur if n is negative.
	Skip(n int) Stream
	// Sequential returns an equivalent stream that is sequential.
	// May return itself, because the stream was already sequential.
	Sequential() Stream
	// Parallel returns an equivalent stream that is parallel.
	// May return itself, because the stream was already parallel.
	Parallel() Stream
}

type sequentialStream struct {
	elements []*element
}

type parallelStream struct {
	elements []*element
}

type errStream struct {
	err      error
	parallel bool
}

type element struct {
	data         interface{}
	reflectValue reflect.Value
}

func NewSequentialStream(data interface{}) Stream {
	elements, err := convertDataToElements(data)
	if err != nil {
		return &errStream{err: err, parallel: false}
	}
	if len(elements) <= 0 {
		return emptySequentialStream
	}
	return &sequentialStream{elements}
}

func NewParallelStream(data interface{}) Stream {
	elements, err := convertDataToElements(data)
	if err != nil {
		return &errStream{err: err, parallel: true}
	}
	if len(elements) <= 0 {
		return emptyParallelStream
	}
	return &parallelStream{elements}
}

func ConcatStream(a, b Stream) (c Stream) {
	var aSlice, bSlice []interface{}
	if err := a.Collect(&aSlice); err != nil {
		return &errStream{err: err}
	}
	aElements, _ := convertDataToElements(aSlice)
	if err := b.Collect(&bSlice); err != nil {
		return &errStream{err: err}
	}
	bElements, _ := convertDataToElements(bSlice)
	return &sequentialStream{elements: append(aElements, bElements...)}
}

func convertDataToElements(data interface{}) ([]*element, error) {
	reflectValue := reflect.ValueOf(data)
	if t := reflectValue.Kind(); t != reflect.Array && t != reflect.Slice {
		return nil, errors.New("cannot new stream with non-slice or non-array")
	}
	length := reflectValue.Len()
	elements := make([]*element, 0, length)
	for i := 0; i < length; i++ {
		reflectValue := reflectValue.Index(i)
		elements = append(elements, &element{
			data:         reflectValue.Interface(),
			reflectValue: reflectValue,
		})
	}
	return elements, nil
}

func (s *sequentialStream) MapToFloat64(mapper func(src interface{}) (dest float64)) Float64Stream {
	if len(s.elements) <= 0 {
		return emptySequentialFloat64Stream
	}
	newElements := make([]float64, 0, len(s.elements))
	for _, e := range s.elements {
		newElements = append(newElements, mapper(e.data))
	}
	return &sequentialFloat64Stream{newElements}
}

func (s *sequentialStream) MapToInt(mapper func(src interface{}) (dest int)) IntStream {
	if len(s.elements) <= 0 {
		return emptySequentialIntStream
	}
	newElements := make([]int, 0, len(s.elements))
	for _, e := range s.elements {
		newElements = append(newElements, mapper(e.data))
	}
	return &sequentialIntStream{newElements}
}

func (s *sequentialStream) IsParallel() bool {
	return false
}

func (s *sequentialStream) Sequential() Stream {
	return s
}

func (s *sequentialStream) Parallel() Stream {
	if len(s.elements) <= 0 {
		return emptyParallelStream
	}
	return &parallelStream{s.elements}
}

func (s *sequentialStream) FlatMap(mapper func(val interface{}) Stream) Stream {
	if len(s.elements) == 0 {
		return s
	}

	newElements := make([]*element, 0, len(s.elements))
	for _, e := range s.elements {
		d := mapper(e.data)
		newElements = append(newElements, &element{data: d, reflectValue: reflect.ValueOf(d)})
	}
	result, _ := (&sequentialStream{elements: newElements}).Reduce(func(a, b interface{}) (c interface{}) {
		return ConcatStream(a.(Stream), b.(Stream))
	})
	return result.(Stream)
}

func (s *sequentialStream) Reduce(accumulator func(a, b interface{}) (c interface{})) (interface{}, error) {
	if len(s.elements) == 0 {
		return nil, nil
	}
	identity := s.elements[0].data
	for i := 1; i < len(s.elements); i++ {
		identity = accumulator(identity, s.elements[i].data)
	}
	return identity, nil
}

func (s *sequentialStream) Skip(n int) Stream {
	if n < 0 {
		return &errStream{err: fmt.Errorf("skip error, n less than 0: %d", n)}
	}
	if len(s.elements) <= n {
		return emptySequentialStream
	}
	return &sequentialStream{elements: s.elements[n:]}
}

func (s *sequentialStream) Limit(maxSize int) Stream {
	if maxSize < 0 {
		return &errStream{err: fmt.Errorf("limit error, maxSize less than 0: %v", maxSize)}
	}
	if maxSize == 0 {
		return emptySequentialStream
	}
	if len(s.elements) <= maxSize {
		return s
	}
	return &sequentialStream{elements: s.elements[:maxSize]}
}

func (s *sequentialStream) Filter(predicate func(val interface{}) (keep bool)) Stream {
	remain := make([]*element, 0)
	for _, elem := range s.elements {
		if predicate(elem.data) {
			remain = append(remain, elem)
		}
	}
	if len(remain) <= 0 {
		return emptySequentialStream
	}
	return &sequentialStream{remain}
}

func (s *sequentialStream) Map(mapper func(src interface{}) (dest interface{})) Stream {
	if len(s.elements) <= 0 {
		return s
	}
	newElements := make([]*element, 0)
	for _, elem := range s.elements {
		newData := mapper(elem.data)
		newElements = append(newElements, &element{
			data:         newData,
			reflectValue: reflect.ValueOf(newData),
		})
	}
	return &sequentialStream{elements: newElements}
}

func (s *sequentialStream) Sorted(less func(a, b interface{}) bool) Stream {
	if len(s.elements) <= 1 {
		return s
	}
	newElements := make([]*element, 0, len(s.elements))
	newElements = append(newElements, s.elements...)
	sort.Slice(newElements, func(i, j int) bool {
		return less(newElements[i].data, newElements[j].data)
	})
	return &sequentialStream{elements: newElements}
}

func (s *sequentialStream) Distinct(hashcode func(obj interface{}) int, equals func(a, b interface{}) bool) Stream {
	if len(s.elements) <= 1 {
		return s
	}
	elementMap := make(map[int][]interface{}, len(s.elements))
	newElements := make([]*element, 0)
	for _, elem := range s.elements {
		code := hashcode(elem.data)
		duplicated := false
		seens := elementMap[code]
		for _, seen := range seens {
			if equals(elem.data, seen) {
				duplicated = true
				break
			}
		}
		if !duplicated {
			elementMap[code] = append(seens, elem.data)
			newElements = append(newElements, elem)
		}
	}
	return &sequentialStream{elements: newElements}
}

func (s *sequentialStream) Collect(collector interface{}) (err error) {
	defer func() {
		// 当stream内的元素类型不能赋值到collector中时会产生panic，要recover处理掉
		if r := recover(); r != nil {
			err = fmt.Errorf("panic when pack results into collector, recover=%v", r)
		}
	}()
	collectorReflectValue := reflect.ValueOf(collector)
	if collectorReflectValue.Kind() != reflect.Ptr || collectorReflectValue.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("cannot pack results into collector, collector is a %s but not a slice pointer", collectorReflectValue.Type())
	}
	results := collectorReflectValue.Elem()
	results.Set(reflect.MakeSlice(results.Type(), 0, len(s.elements)))
	for _, elem := range s.elements {
		if reflectValue := elem.reflectValue; reflectValue.Kind() == reflect.Interface {
			results.Set(reflect.Append(results, reflectValue.Elem()))
		} else {
			results.Set(reflect.Append(results, reflectValue))
		}
	}
	return nil
}

func (s *sequentialStream) Err() error {
	return nil
}

func (p *parallelStream) IsParallel() bool {
	return true
}

func (p *parallelStream) Collect(collector interface{}) (err error) {
	defer func() {
		// 当stream内的元素类型不能赋值到collector中时会产生panic，要recover处理掉
		if r := recover(); r != nil {
			err = fmt.Errorf("panic when pack results into collector, recover=%v", r)
		}
	}()
	collectorReflectValue := reflect.ValueOf(collector)
	if collectorReflectValue.Kind() != reflect.Ptr || collectorReflectValue.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("cannot pack results into collector, collector is a %s but not a slice pointer", collectorReflectValue.Type())
	}
	results := collectorReflectValue.Elem()
	results.Set(reflect.MakeSlice(results.Type(), 0, len(p.elements)))
	for _, elem := range p.elements {
		if reflectValue := elem.reflectValue; reflectValue.Kind() == reflect.Interface {
			results.Set(reflect.Append(results, reflectValue.Elem()))
		} else {
			results.Set(reflect.Append(results, reflectValue))
		}
	}
	return nil
}

func (p *parallelStream) Distinct(hashcode func(obj interface{}) int, equals func(a, b interface{}) bool) Stream {
	if len(p.elements) <= 1 {
		return p
	}
	elementMap := make(map[int][]interface{}, len(p.elements))
	newElements := make([]*element, 0)
	for _, elem := range p.elements {
		code := hashcode(elem.data)
		duplicated := false
		seens := elementMap[code]
		for _, seen := range seens {
			if equals(elem.data, seen) {
				duplicated = true
				break
			}
		}
		if !duplicated {
			elementMap[code] = append(seens, elem.data)
			newElements = append(newElements, elem)
		}
	}
	return &parallelStream{elements: newElements}
}

func (p *parallelStream) Err() error {
	return nil
}

func (p *parallelStream) Filter(predicate func(val interface{}) (match bool)) Stream {
	if len(p.elements) == 0 {
		return p
	}
	remain := make([]*element, 0)
	var wg sync.WaitGroup
	wg.Add(len(p.elements) - 1)
	matches := make([]bool, len(p.elements))
	for i := 1; i < len(p.elements); i++ {
		index := i
		go func() {
			defer wg.Done()
			matches[index] = predicate(p.elements[index].data)
		}()
	}
	if elem := p.elements[0]; predicate(elem.data) {
		remain = append(remain, elem)
	}
	wg.Wait()
	for i := 1; i < len(p.elements); i++ {
		if matches[i] {
			remain = append(remain, p.elements[i])
		}
	}
	return &parallelStream{elements: remain}
}

func (p *parallelStream) FlatMap(mapper func(val interface{}) Stream) Stream {
	if len(p.elements) == 0 {
		return p
	}
	result, _ := p.Map(func(src interface{}) (dest interface{}) {
		dest = mapper(src)
		return dest
	}).Reduce(func(a, b interface{}) (c interface{}) {
		return ConcatStream(a.(Stream), b.(Stream))
	})
	return result.(Stream).Parallel()
}

func (p *parallelStream) Limit(maxSize int) Stream {
	if maxSize < 0 {
		return &errStream{err: errors.New("limit error, because maxSize is negative"), parallel: true}
	}
	if maxSize == 0 {
		return emptyParallelStream
	}
	if maxSize >= len(p.elements) {
		return p
	}
	return &parallelStream{p.elements[:maxSize]}

}

func (p *parallelStream) Map(mapper func(src interface{}) (dest interface{})) Stream {
	length := len(p.elements)
	if length == 0 {
		return p
	}
	calculateElement := func(old *element) (new *element) {
		newData := mapper(old.data)
		return &element{data: newData, reflectValue: reflect.ValueOf(newData)}
	}

	newElements := make([]*element, len(p.elements))
	var wg sync.WaitGroup
	wg.Add(length - 1)
	for i := 1; i < length; i++ {
		index := i
		go func() {
			defer wg.Done()
			newElements[index] = calculateElement(p.elements[index])
		}()
	}
	newElements[0] = calculateElement(p.elements[0])
	wg.Wait()
	return &parallelStream{newElements}
}

func (p *parallelStream) MapToFloat64(mapper func(src interface{}) (dest float64)) Float64Stream {
	length := len(p.elements)
	if length == 0 {
		return emptyParallelFloat64Stream
	}
	newElements := make([]float64, length)
	var wg sync.WaitGroup
	wg.Add(length - 1)
	for i := 1; i < length; i++ {
		index := i
		go func() {
			defer wg.Done()
			newElements[index] = mapper(p.elements[index].data)
		}()
	}
	newElements[0] = mapper(p.elements[0].data)
	wg.Wait()
	return &parallelFloat64Stream{newElements}
}

func (p *parallelStream) MapToInt(mapper func(src interface{}) (dest int)) IntStream {
	length := len(p.elements)
	if length == 0 {
		return emptyParallelIntStream
	}
	newElements := make([]int, length)
	var wg sync.WaitGroup
	wg.Add(length - 1)
	for i := 1; i < length; i++ {
		index := i
		go func() {
			defer wg.Done()
			newElements[index] = mapper(p.elements[index].data)
		}()
	}
	newElements[0] = mapper(p.elements[0].data)
	wg.Wait()
	return &parallelIntStream{newElements}
}

func (p *parallelStream) Reduce(accumulator func(a, b interface{}) (c interface{})) (interface{}, error) {
	if len(p.elements) == 0 {
		return nil, nil
	}
	if len(p.elements) == 1 {
		return p.elements[0].data, nil
	}
	f := newReduceRecursiveTask(accumulator, p.elements, 0, len(p.elements)-1)
	return f.compute(), nil
}

func (p *parallelStream) Sorted(less func(a, b interface{}) bool) Stream {
	if len(p.elements) <= 1 {
		return p
	}
	newElements := make([]*element, len(p.elements))
	copy(newElements, p.elements)
	f := newSortRecursiveAction(func(a, b *element) bool {
		return less(a.data, b.data)
	}, newElements, make([]*element, len(p.elements)), 0, len(newElements)-1)
	f.compute()
	return &parallelStream{newElements}
}

func (p *parallelStream) Skip(n int) Stream {
	if n < 0 {
		return &errStream{err: errors.New("skip error, n is negative")}
	}
	if n == 0 {
		return p
	}
	if n >= len(p.elements) {
		return emptyParallelStream
	}
	return &parallelStream{elements: p.elements[n:]}
}

func (p *parallelStream) Sequential() Stream {
	if len(p.elements) <= 0 {
		return emptySequentialStream
	}
	return &sequentialStream{p.elements}
}

func (p *parallelStream) Parallel() Stream {
	return p
}

func (e *errStream) MapToFloat64(func(src interface{}) (dest float64)) Float64Stream {
	return &errFloat64Stream{err: e.err, parallel: e.parallel}
}

func (e *errStream) MapToInt(func(src interface{}) (dest int)) IntStream {
	return &errIntStream{err: e.err, parallel: e.parallel}
}

func (e *errStream) IsParallel() bool {
	return e.parallel
}

func (e *errStream) Sequential() Stream {
	if e.parallel {
		return &errStream{err: e.err, parallel: false}
	}
	return e
}

func (e *errStream) Parallel() Stream {
	if e.parallel {
		return e
	}
	return &errStream{err: e.err, parallel: true}
}

func (e *errStream) Collect(interface{}) error {
	return e.err
}

func (e *errStream) Distinct(func(obj interface{}) int, func(a, b interface{}) bool) Stream {
	return e
}

func (e *errStream) Err() error {
	return e.err
}

func (e *errStream) Filter(func(val interface{}) (keep bool)) Stream {
	return e
}

func (e *errStream) FlatMap(func(val interface{}) Stream) Stream {
	return e
}

func (e *errStream) Limit(int) Stream {
	return e
}

func (e *errStream) Map(func(src interface{}) (dest interface{})) Stream {
	return e
}

func (e *errStream) Reduce(func(a, b interface{}) (c interface{})) (interface{}, error) {
	return nil, e.err
}

func (e *errStream) Sorted(func(a, b interface{}) bool) Stream {
	return e
}

func (e *errStream) Skip(int) Stream {
	return e
}
