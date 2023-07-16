package btree

import (
	"db/src/constants"
	"db/src/util"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
)

type leafCell struct {
	key  key
	data []byte
}

// leaf node
type LeafNode struct {
	Header *nodeHeader
	Cells  []*leafCell
}

func (ln *LeafNode) maxLeafNodeNumCell() uint32 {
	return nodeBodySize() / ln.Header.CellSize
}

func (ln *LeafNode) SetCellSize(dataSize uint32) {
	ln.Header.CellSize = constants.BTreeKeySize + dataSize
}

// Find the entry in leaf node
func (ln *LeafNode) find(key key) int {
	if len(ln.Cells) == 0 {
		return -1
	} else {
		for index, cell := range ln.Cells {
			if key == cell.key {
				return index
			}
		}
	}
	return -1
}

func (ln *LeafNode) split() {

}

func (ln *LeafNode) insert(key key, data []byte) {

}

func (ln *LeafNode) serialize() []byte {
	page := makeNodePage(constants.MagicNumberLeaf)

	pos := constants.MagicNumberSize // nodes are put after 2 bytes of magic number

	headerBytes := ln.Header.serialize()
	copy(page[pos:pos+nodeHeaderSize()], headerBytes)
	pos = util.AdvanceCursor(pos, nodeHeaderSize())

	cellsBytes, err := ln.serializeCells()
	copy(page[pos:pos+nodeBodySize()], cellsBytes)
	if err != nil {
		log.Fatalln(err)
	}

	return page
}

func (ln *LeafNode) serializeCells() ([]byte, error) {
	cells := ln.Cells
	if len(cells) == 0 || ln.Header.CellSize == 0 {
		return nil, errors.New("serializing leaf node: empty leaf node or cell size not set")
	}
	dataSize := len(cells) * int(ln.Header.CellSize)
	buf := make([]byte, dataSize)
	pos := 0

	for _, cell := range cells {
		binary.LittleEndian.PutUint32(buf[pos:], uint32(cell.key))
		pos = util.AdvanceCursor(pos, constants.BTreeKeySize)
		copy(buf[pos:], cell.data)
		pos = util.AdvanceCursor(pos, len(cell.data))
	}

	return buf, nil
}

func (ln *LeafNode) deserializeCells(bytes []byte) error {
	if len(bytes) < int(ln.Header.CellSize)*int(ln.Header.NumCell) {
		return fmt.Errorf("deserializing leaf node cell: invalid data length -- found %d, expected %d", ln.Header.CellSize, ln.Header.NumCell)
	}

	ln.Cells = make([]*leafCell, ln.Header.NumCell)

	var pos uint32 = 0
	for i := 0; i < int(ln.Header.NumCell); i++ {
		data := make([]byte, ln.Header.CellSize)
		copy(data, bytes[pos+constants.BTreeKeySize:pos+ln.Header.CellSize])
		ln.Cells[i] = &leafCell{
			key:  key(binary.LittleEndian.Uint32(bytes[pos : pos+constants.BTreeKeySize])),
			data: data,
		}
		pos = util.AdvanceCursor(pos, ln.Header.CellSize)
	}
	return nil
}

func (ln *LeafNode) deserialize(bytes []byte) error {
	if len(bytes) != int(constants.PageSize) {
		return fmt.Errorf("deserializing leaf node: invalid bytes size -- %d, expected %d", len(bytes), constants.PageSize)
	}
	magicNumber := hex.EncodeToString(bytes[:constants.MagicNumberSize])
	if magicNumber != constants.MagicNumberLeaf {
		return fmt.Errorf("deserializing leaf node: invalid magic number for leaf node -- %s, expected %s", magicNumber, constants.MagicNumberLeaf)
	}
	pos := constants.MagicNumberSize
	ln.Header.deserialize(bytes[pos : pos+nodeHeaderSize()])
	pos = util.AdvanceCursor(pos, nodeHeaderSize())
	err := ln.deserializeCells(bytes[pos : pos+ln.Header.CellSize*uint32(ln.Header.NumCell)])
	if err != nil {
		return err
	}
	return nil
}
