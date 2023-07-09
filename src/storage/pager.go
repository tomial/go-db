package storage

import "os"

const dbFileName string = "./my.db"

const pageSize uint = 4096

type Pager struct {
	File *os.File
}
