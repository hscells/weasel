package weasel

import "reflect"

type Indexable reflect.Kind

var (
	IndexableString      = Indexable(reflect.String)
	IndexableStringSlice = Indexable(reflect.Slice)
)
