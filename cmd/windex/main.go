package main

import (
	"github.com/alexflint/go-arg"
	"github.com/hscells/weasel"
	"reflect"
)

type args struct {
	Name      string `arg:"help:name of index and path to index.,required"`
	Documents string `arg:"help:path to documents.,required"`
}

func (args) Version() string {
	return "weasel indexer 5.Feb.2018"
}

func (args) Description() string {
	return `Index documents into weasel.`
}

func main() {

	var args args
	arg.MustParse(&args)

	wi := weasel.NewInvertedIndex(args.Name, map[string]reflect.Kind{"text": reflect.String, "title": reflect.String})

	// TODO implement indexing.

	wi.Dump()
}
