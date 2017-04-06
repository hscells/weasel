package main

import (
	"github.com/hscells/weasel/index"
	"log"
	"github.com/hscells/weasel/document"
	"github.com/hscells/weasel/query"
	"reflect"
	"path/filepath"
	"os"
	"strings"
	"encoding/xml"
)

func main() {
	mapping := make(map[string]reflect.Kind)
	mapping["abstract"] = reflect.String

	invertedIndex := index.New("med_test3", mapping)

	fileList := []string{}

	err := filepath.Walk("citations5", func(path string, f os.FileInfo, err error) error {
		fileList = append(fileList, path)
		return nil
	})

	for _, file := range fileList {
		if strings.Contains(file, ".xml") && !strings.Contains(file, ".gz") {
			var docs []document.Document

			log.Println(file)
			f, err := os.Open(file)
			if err != nil {
				log.Panicln(err)
			}
			decoder := xml.NewDecoder(f)

			log.Println("parsing xml...")
			for {
				t, _ := decoder.Token()
				if t == nil {
					break
				}

				switch se := t.(type) {
				case xml.StartElement:
					if se.Name.Local == "AbstractText" {
						var text string
						err := decoder.DecodeElement(&text, &se)
						if err != nil {
							log.Panicln(err)
						}
						d := document.New()
						d.Set("abstract", text)
						docs = append(docs, d)
					}
				}
			}

			log.Printf("indexing %v\n", file)
			err = invertedIndex.BulkIndex(docs)
			if err != nil {
				log.Panicln(err)
			}
		}
	}

	err = invertedIndex.Dump()
	if err != nil {
		log.Panicln(err)
	}

	err = invertedIndex.DumpDocs()
	if err != nil {
		log.Panicln(err)
	}

	log.Println("Loading index!")

	i, err := index.Load("med_test3.weasel")
	if err != nil {
		log.Panicln(err)
	}

	log.Println("Loaded index!")
	log.Printf("%v documents in the index", i.NumDocs)
	log.Printf("%v terms in the index", len(i.TermMapping))

	q := query.BooleanQuery{
		Operator: query.And,
		QueryTerms: []string{"human", "babies"},
		Field: "abstract",
	}

	log.Println("Start querying!")

	docs, err := q.Query(i)
	if err != nil {
		log.Panicln(err)
	}

	log.Println("Finished querying!")

	log.Printf("%v docs retrieved\n", len(docs))

	log.Println("Getting sources")

	sources, err := i.GetSources(docs)
	if err != nil {
		log.Panicln(err)
	}

	log.Println("Got sources!")

	for _, doc := range sources {
		source := document.From(doc.Source)
		log.Println(doc.Id)
		log.Printf("%v", source.Get("abstract"))
	}
}


