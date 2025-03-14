package btree

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/tomial/go-db/internal/constants"
	"github.com/tomial/go-db/internal/util"
)

type leafCell struct {
	key  key
	data []byte
}

// leaf node
type LeafNode struct {
	btree  *BTree
	Header *nodeHeader
	Cells  []*leafCell
}

func initEmptyLeafNode() *LeafNode {
	return &LeafNode{Header: initHeader(TypeLeaf)}
}

func initEmptyRootNode() *LeafNode {
	ln := &LeafNode{Header: initHeader(TypeRoot)}
	ln.Header.Page = 1
	return ln
}

func (ln *LeafNode) maxLeafNodeNumCell() uint32 {
	return nodeBodySize() / ln.Header.CellSize
}

func (ln *LeafNode) SetCellSize(dataSize uint32) {
	ln.Header.CellSize = constants.BTreeKeySize + dataSize
}

// Find the entry in leaf node
func (ln *LeafNode) find(key key) (found bool, data []byte) {
	if len(ln.Cells) == 0 {
		return false, nil
	} else {
		for _, cell := range ln.Cells {
			if cell == nil {
				return false, nil
			}
			if key == cell.key {
				return true, cell.data
			}
		}
	}
	return false, nil
}

// the caller leaf node is the target, return itself
func (ln *LeafNode) searchLeaf(key key) *LeafNode {
	return ln
}

// return the split node
func (ln *LeafNode) split(key key) *LeafNode {
	right := initEmptyLeafNode()
	right.Header.CellSize = ln.Header.CellSize
	right.Header.Parent = ln.Header.Parent

	// init tree for metadata(next page to insert)
	if ln.btree == nil {
		ln.btree = NewBtree()
		ln.btree.deserialize(ln.btree.pager.ReadPage(0))
		right.btree = ln.btree
	}
	right.Header.Page = PageNum(ln.btree.NumNode + 1)
	ln.btree.NumNode++
	right.Cells = make([]*leafCell, right.maxLeafNodeNumCell())

	// move half of the old node cells to the right
	copy(right.Cells, ln.Cells[ln.Header.NumCell/2:])
	for i := ln.Header.NumCell / 2; i < ln.Header.NumCell; i++ {
		ln.Cells[i] = nil
	}
	right.Header.NumCell = (ln.Header.NumCell - ln.Header.NumCell/2)
	ln.Header.NumCell -= (ln.Header.NumCell - ln.Header.NumCell/2)

	right.Header.Next = ln.Header.Next
	ln.Header.Next = right.Header.Page

	// ln is the original root node
	if ln.Header.Parent == 0 {
		newRoot := initEmptyInternalNode()
		newRoot.btree = ln.btree

		// ln become the leftmost node
		ln.btree.First = ln.Header.Page

		newRoot.Header.Page = PageNum(ln.btree.NumNode + 1)
		ln.Header.Parent = newRoot.Header.Page
		right.Header.Parent = newRoot.Header.Page

		ln.btree.Root = newRoot.Header.Page
		ln.btree.NumNode++

		newRoot.Cells = make([]*internalCell, maxInternalNodeNumCell())
		newRoot.Cells[0] = &internalCell{
			key:  right.Cells[0].key,
			left: ln.Header.Page,
		}
		newRoot.Header.NumCell++
		if key < right.Cells[0].key {
			newRoot.Cells[0].key = key // will be inserted into right's leftmost
		}
		newRoot.Cells[0].right = right.Header.Page

		ln.btree.save()
		newRoot.save()
	} else {
		// the key that goes to the parent
		bubbleKey := right.Cells[0].key
		if key < bubbleKey {
			bubbleKey = key
		}

		parent := ln.btree.readNode(ln.Header.Parent)
		internalCell := &internalCell{
			key:   bubbleKey,
			left:  ln.Header.Page,
			right: right.Header.Page,
		}

		bytes, err := internalCell.serialize()
		if err != nil {
			log.Fatal(err)
		}

		ln.btree.save()
		parent.saveCell(bubbleKey, bytes)
	}

	return right
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
		if cell != nil {
			binary.LittleEndian.PutUint32(buf[pos:], uint32(cell.key))
			pos = util.AdvanceCursor(pos, constants.BTreeKeySize)
			copy(buf[pos:], cell.data)
			pos = util.AdvanceCursor(pos, len(cell.data))
		}
	}

	return buf, nil
}

func (ln *LeafNode) deserializeCells(bytes []byte) error {
	if len(bytes) < int(ln.Header.CellSize)*int(ln.Header.NumCell) {
		return fmt.Errorf("deserializing leaf node cell: invalid data length -- found %d, expected %d", ln.Header.CellSize, ln.Header.NumCell)
	}

	ln.Cells = make([]*leafCell, ln.maxLeafNodeNumCell())

	var pos uint32 = 0
	for i := 0; i < int(ln.Header.NumCell); i++ {
		data := make([]byte, ln.Header.CellSize-constants.BTreeKeySize)
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
	err := ln.Header.deserialize(bytes[pos : pos+nodeHeaderSize()])
	if err != nil {
		return err
	}
	pos = util.AdvanceCursor(pos, nodeHeaderSize())
	err = ln.deserializeCells(bytes[pos : pos+ln.Header.CellSize*uint32(ln.Header.NumCell)])
	if err != nil {
		return err
	}
	return nil
}

func (ln *LeafNode) saveCell(key key, data []byte) {
	pos := 0
	for index, cell := range ln.Cells {
		if cell == nil || cell.key >= key {
			pos = index
			break
		}
	}

	var right *LeafNode
	if ln.Header.NumCell == uint8(ln.maxLeafNodeNumCell()) {
		right = ln.split(key)
	}

	// if split, add cell at the right node
	if right != nil {
		right.saveCell(key, data)
		right.save()
	} else {
		// move all elements to the right if there's any elements
		if ln.Cells[pos] != nil {
			copy(ln.Cells[pos+1:], ln.Cells[pos:])
		}
		ln.Cells[pos] = &leafCell{
			key:  key,
			data: data,
		}
		ln.Header.NumCell += 1
		if ln.Header.NumCell == uint8(ln.maxLeafNodeNumCell()) {
			right = ln.split(key)
			right.save()
		}
	}

	ln.save()
}

func (ln *LeafNode) save() error {
	if ln.btree == nil {
		ln.btree = NewBtree()
	}

	if ln.Header.Page == 0 {
		return fmt.Errorf("saving internal node: invalid node page: %d", ln.Header.Page)
	}

	ln.btree.pager.WritePage(uint32(ln.Header.Page), ln.serialize())

	return nil
}
