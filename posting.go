package weasel

import (
	"os"
	"path"
	"fmt"
	"io/ioutil"
)

type Posting struct {
	d string
}

func NewPosting(directory string, fields ...string) (Posting, error) {
	for _, field := range fields {
		os.Mkdir(path.Join(directory, field), 0777)
	}
	return Posting{
		directory,
	}, nil
}

func (p Posting) Write(field string, termID TermID, value []byte) error {
	return ioutil.WriteFile(path.Join(p.d, field, fmt.Sprintf("%d", termID)), value, 0644)
}

func (p Posting) Read(field string, termID TermID) ([]byte, error) {
	return ioutil.ReadFile(path.Join(p.d, field, fmt.Sprintf("%d", termID)))
}
