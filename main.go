package main

import (
	"os"
	"github.com/hscells/weasel/index"
	"log"
	"github.com/hscells/weasel/document"
	"bufio"
	"fmt"
	"encoding/json"
	"bytes"
	"github.com/hscells/weasel/api"
)

func main() {
	args := os.Args[1:]

	indexName := args[0]

	fmt.Println("Loading index...")

	// Load the index from disk
	i, err := index.Load(indexName)
	if err != nil {
		log.Panicln(err)
	}

	fmt.Println("Ready!")

	reader := bufio.NewReader(os.Stdin)

	for true {

		fmt.Print("> ")
		queryText, _ := reader.ReadString('\n')

		b := bytes.NewBufferString(queryText)

		// Construct a boolean query
		var w api.BooleanQueryWrapper
		json.Unmarshal(b.Bytes(), &w)

		q := w.Convert()

		// Query the index to retrieve documents
		docs, err := q.Query(i)
		if err != nil {
			log.Panicln(err)
		}

		// Print the id and body of each document retrieved
		for j := 0; j < 10; j++ {
			if j < len(docs) {
				doc, _ := i.Get(docs[j])
				source := document.From(doc.Source)
				fmt.Printf("%v\n----\n", source)
			}

		}

		if len(docs) == 0 {
			fmt.Println("0 Results.")
		} else {
			fmt.Printf("%v Results.\n", len(docs))
		}
	}
}


