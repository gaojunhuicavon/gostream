package gostream

import "sync"

type recursiveAction struct {
	compute func()
	wg      sync.WaitGroup
}

type recursiveTask struct {
	*recursiveAction
	compute func() interface{}
	result  interface{}
}

type reduceRecursiveTask struct {
	*recursiveTask
	accumulator func(a, b interface{}) interface{}
	elements    []*element
	start, end  int
}

type sortRecursiveAction struct {
	*recursiveAction
	less       func(a, b *element) bool
	elements   []*element
	aux        []*element
	start, end int
}

type intReduceRecursiveTask struct {
	*recursiveTask
	accumulator func(a, b int) int
	elements    []int
	start, end  int
}

type f64ReduceRecursiveTask struct {
	*recursiveTask
	accumulator func(a, b float64) float64
	elements    []float64
	start, end  int
}

func newRecursiveTask() *recursiveTask {
	r := &recursiveTask{
		recursiveAction: &recursiveAction{},
	}
	r.recursiveAction.compute = r.actualCompute
	return r
}

func newReduceRecursiveTask(accumulator func(a, b interface{}) interface{}, elements []*element, start, end int) *reduceRecursiveTask {
	r := &reduceRecursiveTask{
		recursiveTask: newRecursiveTask(),
		accumulator:   accumulator,
		elements:      elements,
		start:         start,
		end:           end,
	}
	r.recursiveTask.compute = r.compute
	return r
}

func newSortRecursiveAction(less func(a, b *element) bool, elements, aux []*element, start, end int) *sortRecursiveAction {
	s := &sortRecursiveAction{
		recursiveAction: &recursiveAction{},
		less:            less,
		elements:        elements,
		aux:             aux,
		start:           start,
		end:             end,
	}
	s.recursiveAction.compute = s.compute
	return s
}

func newIntReduceRecursiveTask(accumulator func(a, b int) int, elements []int, start, end int) *intReduceRecursiveTask {
	i := &intReduceRecursiveTask{
		recursiveTask: newRecursiveTask(),
		accumulator:   accumulator,
		elements:      elements,
		start:         start,
		end:           end,
	}
	i.recursiveTask.compute = i.compute
	return i
}

func newFloat64ReduceRecursiveTask(accumulator func(a, b float64) float64, elements []float64, start, end int) *f64ReduceRecursiveTask {
	newRecursiveTask()
	f := &f64ReduceRecursiveTask{
		recursiveTask: newRecursiveTask(),
		accumulator:   accumulator,
		elements:      elements,
		start:         start,
		end:           end,
	}
	f.recursiveTask.compute = f.compute
	return f
}

func (r *recursiveAction) fork() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.compute()
	}()
}

func (r *recursiveAction) join() {
	r.wg.Wait()
}

func (r *recursiveTask) actualCompute() {
	r.result = r.compute()
}

func (r *recursiveTask) join() interface{} {
	r.recursiveAction.join()
	return r.result
}

func (r *reduceRecursiveTask) compute() interface{} {
	length := r.end - r.start + 1
	if length == 1 {
		return r.elements[r.start].data
	}
	if length == 2 {
		return r.accumulator(r.elements[r.start].data, r.elements[r.end].data)
	}
	mid := (r.start + r.end) >> 1
	left := newReduceRecursiveTask(r.accumulator, r.elements, r.start, mid)
	left.fork()
	right := newReduceRecursiveTask(r.accumulator, r.elements, mid+1, r.end).compute()
	return r.accumulator(left.join(), right)
}

func (s *sortRecursiveAction) compute() {
	length := s.end - s.start + 1
	if length <= 1 {
		return
	}
	if length == 2 {
		if s.less(s.elements[s.end], s.elements[s.start]) {
			tmp := s.elements[s.start]
			s.elements[s.start] = s.elements[s.end]
			s.elements[s.end] = tmp
		}
		return
	}
	mid := (s.start + s.end) >> 1
	left := newSortRecursiveAction(s.less, s.elements, s.aux, s.start, mid)
	left.fork()
	right := newSortRecursiveAction(s.less, s.elements, s.aux, mid+1, s.end)
	right.compute()
	left.join()

	// merge
	for i := s.start; i <= s.end; i++ {
		s.aux[i] = s.elements[i]
	}
	i, j := s.start, mid+1
	for k := s.start; k <= s.end; k++ {
		if i > mid {
			s.elements[k] = s.aux[j]
			j++
		} else if j > s.end {
			s.elements[k] = s.aux[i]
			i++
		} else if s.less(s.aux[i], s.aux[j]) {
			s.elements[k] = s.aux[i]
			i++
		} else {
			s.elements[k] = s.aux[j]
			j++
		}
	}
}

func (i *intReduceRecursiveTask) compute() interface{} {
	length := i.end - i.start + 1
	if length == 1 {
		return i.elements[i.start]
	}
	if length == 2 {
		return i.accumulator(i.elements[i.start], i.elements[i.end])
	}
	mid := (i.start + i.end) >> 1
	left := newIntReduceRecursiveTask(i.accumulator, i.elements, i.start, mid)
	left.fork()
	right := newIntReduceRecursiveTask(i.accumulator, i.elements, mid+1, i.end).compute()
	return i.accumulator(left.join().(int), right.(int))
}

func (f *f64ReduceRecursiveTask) compute() interface{} {
	length := f.end - f.start + 1
	if length == 1 {
		return f.elements[f.start]
	}
	if length == 2 {
		return f.accumulator(f.elements[f.start], f.elements[f.end])
	}
	mid := (f.start + f.end) >> 1
	left := newFloat64ReduceRecursiveTask(f.accumulator, f.elements, f.start, mid)
	left.fork()
	right := newFloat64ReduceRecursiveTask(f.accumulator, f.elements, mid+1, f.end).compute()
	return f.accumulator(left.join().(float64), right.(float64))
}
