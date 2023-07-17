package btree

import (
	"db/src/constants"
	"encoding/hex"
	"log"
)

type node interface {
	serialize() []byte
	deserialize(bytes []byte) error
	serializeCells() ([]byte, error)
	deserializeCells(bytes []byte) error
	find(key key) (found bool, data []byte)
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
