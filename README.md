# weasel

weasel is a toy search engine that implements many common functions of a
typical search engine. It can handle millions of documents and supports
unicode text, bulk indexing, boolean queries and document statistics.

Here is an example of using weasel.

Creating an index:

```go
// Create the document mapping
mapping := make(map[string]reflect.Kind)
mapping["body"] = reflect.String
// Create the index using the mapping
i := index.New("my_index", mapping)
// Create a new document
d := document.New()
d.Set("body", "The cat in the hat is a childrens book about...")
// Index the document
err = i.Index(d)
if err != nil {
    log.Panicln(err)
}
// Dump the index to disk
err = invertedIndex.Dump()
if err != nil {
    log.Panicln(err)
}
// Dump the documents to disk
err = invertedIndex.DumpDocs()
if err != nil {
    log.Panicln(err)
}
```

Note that when using the bulk index method, this will dump documents after
a certain threshold.

Searching an index:

```go
// Load the index from disk
i, err := index.Load("my_index.weasel")
if err != nil {
    log.Panicln(err)
}
// Construct a boolean query
q := query.BooleanQuery{
    Operator: query.And,
    QueryTerms: []string{"cat", "hat"},
    Field: "body",
}
// Query the index to retrieve documents
docs, err := q.Query(i)
if err != nil {
    log.Panicln(err)
}
// Get the sources of the documents retrieved
sources, err := i.GetSources(docs)
if err != nil {
    log.Panicln(err)
}
// Print the id and body of each document retrieved
for _, doc := range sources {
    source := document.From(doc.Source)
    log.Println(doc.Id)
    log.Printf("%v", source.Get("body"))
}
```