package storage

import (
	"db/src/btree"
	"errors"
	"fmt"
)

type Table struct {
	Name   string
	BTree  *btree.BTree
	RowNum uint32
}

func (t *Table) Persist(data []byte, key uint32) error {
	t.BTree.Insert(key, data)

	return nil
}

func (t *Table) Load(key uint32) ([]byte, error) {
	if key == 0 {
		return nil, errors.New("loading data: invalid index 0")
	}
	found, data := t.BTree.Search(key)
	if found {
		return data, nil
	} else {
		return nil, fmt.Errorf("error loading table: key %d not found", key)
	}
}

func InitTable(name string) *Table {
	btree := btree.NewBtree()
	return &Table{
		Name:  name,
		BTree: btree,
	}
}

func (t *Table) String() string {
	return t.Name
}
