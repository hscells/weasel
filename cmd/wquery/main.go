package main

import (
	"github.com/alexflint/go-arg"
	"github.com/hscells/cqr"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/weasel"
	"log"
	"fmt"
	"time"
)

type args struct {
	Index string `arg:"help:path to weasel index store.,required"`
	Query string `arg:"help:query to issue to weasel index."`
}

func (args) Version() string {
	return "weasel query 5.Feb.2018"
}

func (args) Description() string {
	return `Query weasel indexes.`
}

func main() {

	var args args
	arg.MustParse(&args)

	p := pipeline.NewPipeline(parser.NewCQRParser(),
		backend.NewCQRBackend(),
		pipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: false,
			},
			RequiresLexing: false,
		})

	query, err := p.Execute(args.Query)
	if err != nil {
		log.Fatal(err)
	}

	repr, err := query.Representation()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(repr)

	log.Println("parsed query")

	wi, err := weasel.LoadIndex(args.Index)
	if err != nil {
		panic(err)
	}

	log.Println("loaded index")

	start := time.Now()
	tree, _, err := wi.Execute(repr.(cqr.CommonQueryRepresentation))
	if err != nil {
		panic(err)
	}

	log.Println("executed query")

	docs, err := tree.Documents()
	if err != nil {
		panic(err)
	}

	elapsed := time.Since(start)

	log.Printf("retrieved %v documents in %v seconds", len(docs), elapsed.Seconds())

	max := 10
	for i, docID := range docs {
		doc, err := wi.Get(docID)
		if err != nil {
			panic(err)
		}
		fmt.Printf("[%v] - %v\n", doc.ID, doc.Source["title"])
		if i > max {
			break
		}
	}

	log.Println("done")
}
