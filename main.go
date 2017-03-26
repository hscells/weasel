package main

import (
	"github.com/hscells/weasel/index"
	"github.com/hscells/weasel/document"
	"reflect"
	"log"
	"github.com/hscells/weasel/query"
)

func main() {

	d1 := document.New()
	err := d1.Set("title", "This is the  title of my document!")
	if err != nil {
		log.Panic(err)
	}

	d2 := document.New()
	err = d2.Set("title", "This is my awesome document!  Yeah!")
	if err != nil {
		log.Panic(err)
	}

	mapping := make(map[string]reflect.Kind)
	mapping["title"] = reflect.String

	invertedIndex := index.New("test", mapping)
	err = invertedIndex.Index(d1)
	if err != nil {
		log.Panic(err)
	}

	err = invertedIndex.Index(d2)
	if err != nil {
		log.Panic(err)
	}

	err = invertedIndex.Dump()
	if err != nil {
		log.Panicln(err)
	}

	log.Printf("%v", invertedIndex.TermMapping)
	log.Printf("%v", invertedIndex.InvertedIndex)

	q := query.BooleanQuery{
		Operator: query.And,
		QueryTerms: []string{"this", "yeah"},
		Field: "title",
		Children: []query.BooleanQuery{
			{
				Operator: query.Or,
				QueryTerms: []string{"title"},
				Field: "title",
			},
		},
	}

	docs := q.Query(invertedIndex)
	log.Println(len(docs))
	for _, d := range docs {
		log.Println(d.Get("title"))
	}

}


