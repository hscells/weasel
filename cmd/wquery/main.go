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
)

type args struct {
	IndexFile  string `arg:"help:path to weasel index file.,required"`
	IndexStore string `arg:"help:path to weasel index store.,required"`
	Query      string `arg:"help:query to issue to weasel index."`
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

	wi, err := weasel.LoadIndex(args.IndexFile, args.IndexStore)
	if err != nil {
		log.Fatal(err)
	}

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

	seen := make(map[uint64]weasel.LogicalTreeNode)
	tree, _, err := wi.Execute(repr.(cqr.CommonQueryRepresentation), seen)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Documents")
	log.Println(len(tree.Documents()))

}
