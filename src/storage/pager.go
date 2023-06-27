package storage

import "os"

const dbFileName string = "./my.db"

const pageSize uint = 4096

type pager struct {
	file *os.File
}
