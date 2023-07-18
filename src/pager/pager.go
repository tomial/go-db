package pager

import (
	"db/src/constants"
	"io"
	"log"
	"os"
)

type Pager struct {
	File *os.File
}

func Init(file *os.File) *Pager {
	return &Pager{File: file}
}

func (p *Pager) Fstat() os.FileInfo {
	fstat, err := p.File.Stat()
	if err != nil {
		log.Fatalf("Pager: failed to read database file stat %s -- %s", constants.DbFileName, err)
	}
	return fstat
}

func (p *Pager) WritePage(page uint32, data []byte) {

	pageSize := len(data)

	if pageSize != int(constants.PageSize) {
		log.Fatalf("Pager: writing page, invalid page size: %d\n", pageSize)
	}

	offset := io.SeekStart + page*constants.PageSize
	n, err := p.File.WriteAt(data, int64(offset))
	if n != int(constants.PageSize) {
		log.Fatalf("Pager: failed to write all page data, wrote %d\n", n)
	}

	if err != nil {
		log.Fatalf("Pager: failed to write page: %s\n", err.Error())
	}

}

// page start from 1, page 0 is for tree struct
func (p *Pager) ReadPage(page uint32) []byte {
	pageBuf := make([]byte, constants.PageSize)

	offset := io.SeekStart + page*constants.PageSize
	n, err := p.File.ReadAt(pageBuf, int64(offset))

	if n != int(constants.PageSize) {
		log.Fatalf("Pager: read incomplete page sized %d at page %d\n", n, page)
	}

	if err != nil {
		log.Fatalf("Pager: failed to seek page %d offset -- %s\n", page, err.Error())
	}

	return pageBuf
}
