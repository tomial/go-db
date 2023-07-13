package pager

import (
	"db/src/constants"
	"log"
	"os"
)

type Pager struct {
	File *os.File
}

func Init() *Pager {
	file, err := os.OpenFile(constants.DbFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Fatalf("Pager: failed to open database file %s -- %s", constants.DbFileName, err)
	}

	return &Pager{File: file}
}

func (p *Pager) Fstat() os.FileInfo {
	fstat, err := p.File.Stat()
	if err != nil {
		log.Fatalf("Pager: failed to read database file stat %s -- %s", constants.DbFileName, err)
	}
	return fstat
}
