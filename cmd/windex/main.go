package main

import (
	"github.com/alexflint/go-arg"
	"github.com/hscells/weasel"
	"gopkg.in/olivere/elastic.v5"
	"io"
	"encoding/json"
	"log"
	"context"
	"time"
	"fmt"
	"os"
	"runtime/pprof"
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
	f, err := os.Create("weasel.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	var args args
	arg.MustParse(&args)
	wi := weasel.NewInvertedIndex(args.Name,
		map[string]weasel.Indexable{
			"text":         weasel.IndexableString,
			"text.stemmed": weasel.IndexableString,
			"title":        weasel.IndexableString,
		},
		weasel.NoSource("text.stemmed"),
		weasel.AddAnalyser("title", weasel.LowercaseFilter, weasel.StopEn),
		weasel.AddAnalyser("text", weasel.LowercaseFilter, weasel.StopEn),
		weasel.AddAnalyser("text.stemmed", weasel.Porter2Stemmer, weasel.LowercaseFilter, weasel.StopEn))

	client, err := elastic.NewClient(elastic.SetURL("http://localhost:9200"))
	if err != nil {
		log.Fatal(err)
	}

	// Scroll search.
	svc := client.Scroll("ecir2018").
		FetchSource(false).
		Pretty(false).
		Type("doc").
		KeepAlive("1h").
		SearchSource(
		elastic.NewSearchSource().
			From(0).
			NoStoredFields().
			FetchSource(true).
			Size(10000).
			TrackScores(false).
			Query(elastic.NewMatchAllQuery()))

	docs := 0

	docsSince := 0
	start := time.Now()

	for docs < 28000 {
		result, err := svc.Do(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("indexing next set of documents (%v) %v indexed so far\n", len(result.Hits.Hits), docs)

		for _, hit := range result.Hits.Hits {

			doc := weasel.NewDocument(hit.Id)
			var source map[string]string
			b, err := hit.Source.MarshalJSON()
			if err != nil {
				log.Fatal(err)
			}
			err = json.Unmarshal(b, &source)

			if v, ok := source["title"]; ok {
				doc.Set("title", v)
			}

			if v, ok := source["text"]; ok {
				doc.Set("text", v)
				doc.Set("text.stemmed", v)
			}

			err = wi.Index(doc)
			if err != nil {
				panic(err)
			}
			docs++
			docsSince++

			if docs > 28000 {
				break
			}

			if time.Since(start) > 1*time.Second {
				fmt.Printf("indexing at %v docs/second\n", docsSince)
				start = time.Now()
				docsSince = 0
			}
		}
	}

	err = svc.Clear(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("final size of the collection: %v\n", len(wi.DocumentMapping))

	wi.Dump()
}
