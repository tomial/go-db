package btree

import (
	"db/src/constants"
	"encoding/binary"
)

type leafCell struct {
	key  key
	data []byte
}

// leaf node
type LeafNode struct {
	nodeHeader
	Cells []leafCell
}

func (ln *LeafNode) maxLeafNodeNumCell() uint32 {
	return nodeBodySize() / ln.CellSize
}

func (ln *LeafNode) setCellSize(dataSize uint32) {
	ln.CellSize = constants.BTreeKeySize + dataSize
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
	copy(page[constants.MagicNumberSize:], ln.serializeCells())
	return page
}

func (ln *LeafNode) serializeCells() []byte {
	cells := ln.Cells
	dataSize := len(cells) * int(ln.CellSize)
	buf := make([]byte, dataSize)
	pos := 0

	for _, cell := range cells {
		binary.LittleEndian.PutUint32(buf[pos:], uint32(cell.key))
		pos += constants.BTreeKeySize
		copy(buf[pos:], cell.data)
		pos += len(cell.data)
	}

	return buf
}

func deserialize([]byte) *LeafNode {
	return nil
}
