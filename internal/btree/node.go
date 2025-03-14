package btree

import (
	"encoding/hex"
	"log"

	"github.com/tomial/go-db/internal/constants"
)

type node interface {
	serialize() []byte
	deserialize(bytes []byte) error
	serializeCells() ([]byte, error)
	deserializeCells(bytes []byte) error
	find(key key) (found bool, data []byte)
	saveCell(key key, data []byte)
	searchLeaf(key key) *LeafNode
}

const (
	TypeInternal NodeType = iota
	TypeLeaf
	TypeRoot
	TypeInvalid
)

func makeNodePage(magicNumberStr string) []byte {
	buf := make([]byte, constants.PageSize)
	magicNumber, err := hex.DecodeString(magicNumberStr)
	if err != nil {
		log.Fatalf("Btree leaf: failed to decode magic number bytes -- %s\n", err.Error())
	}
	copy(buf, magicNumber)
	return buf
}
