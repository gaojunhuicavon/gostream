# GOSTREAM

Gostream is a  Java like stream API written in Go. It support functional-style operations on streams of elements, such as map-reduce transformations on collections.

[![Build Status](https://travis-ci.org/gaojunhuicavon/gostream.svg)](https://travis-ci.org/gaojunhuicavon/gostream)
[![codecov](https://codecov.io/gh/gaojunhuicavon/gostream/branch/master/graph/badge.svg)](https://codecov.io/gh/gaojunhuicavon/gostream)
[![GoDoc](https://godoc.org/github.com/gaojunhuicavon/gostream?status.svg)](https://pkg.go.dev/github.com/gaojunhuicavon/gostream?tab=doc)

## Installation

1. The first need Go installed (version 1.14+ is required), then you can use the below Go command to install Gostream.

```
$ go get github.com/gaojunhuicavon/gostream
```

2. Import it in your code:

```go
import "github.com/gaojunhuicavon/gostream"
```

## Example

```go
package main

import (
	"github.com/gaojunhuicavon/gostream"
	"log"
)

const (
	red color = iota
	green
	yellow
)

type color int

type widget struct {
	color  color
	weight int
}

func main() {
	widgets := []*widget{{red, 1}, {green, 2}, {yellow, 3}}
	max, err := gostream.NewSequentialStream(widgets).Filter(func(val interface{}) (match bool) {
		return val.(*widget).color == red
	}).MapToInt(func(src interface{}) (dest int) {
		return src.(*widget).weight
    }).Max()
	if err != nil {
		log.Printf("get an error: %s\n", err.Error())
		return
	}
	if max != nil {
		log.Printf("the max weight is %d\n", *max)
	}
}
```

