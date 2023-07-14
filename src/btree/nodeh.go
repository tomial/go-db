package btree

import (
	"db/src/constants"
	"db/src/util"
	"encoding/binary"
	"fmt"
	"reflect"
)

// A simple b plus tree implementation
// order 8 example :
//                                      ┌──┬──┬──┬──┐
//                                      │02│--│--│--│
//                                      ├──┼──┼──┼──┼──┐
//                          ┌───────────┤LR│LR│LR│LR│N-│
//                          │           └─┬┴──┴──┴──┴──┘
//                          │             │
//                          ▼             │
//                         ┌──┬──┬──┬──┐  └──────────►┌──┬──┬──┬──┐
//                         │01│--│--│--│              │02│03│--│--│
//                         ├──┼──┼──┼──┼──┐           ├──┼──┼──┼──┼──┐
//                         │D │D │D │D │N ├──────────►│D │D │D │D │N-│
//                         └──┴──┴──┴──┴──┘           └──┴──┴──┴──┴──┘

type NodeType uint8
type PageNum uint32

const (
	TypeInternal NodeType = iota
	TypeLeaf
	TypeRoot
)

// Common fields of leaf and internal node
type nodeHeader struct {
	// Headers: 15B
	Typ      NodeType // 1B
	Parent   PageNum  // 4B Pointer to parent node (read actual struct with PageNum)
	Next     PageNum  // 4B Pointer to next leaf node (page 0 is tree struct, used as nil here)
	CellSize uint32   // 4B Size of node cell, The cell size of leaf node depends on what table(row) it stores
	Height   uint8
	NumCell  uint8 // 1B Amount of cells(cell content : internal - pointer to child, leaf - data)
}

func nodeHeaderSize() uint32 {
	nh := nodeHeader{}
	val := reflect.ValueOf(nh)
	hnum := val.NumField()

	var size uint32 = 0
	for i := 0; i < hnum; i++ {
		size += uint32(val.Field(i).Type().Size())
	}
	return size
}

func nodeBodySize() uint32 {
	return uint32(constants.PageSize) - nodeHeaderSize() - constants.MagicNumberSize
}

func (header nodeHeader) serialize() []byte {
	buf := make([]byte, nodeHeaderSize())

	val := reflect.ValueOf(header)
	pos := 0
	fieldNum := val.NumField()
	for i := 0; i < fieldNum; i += 1 {
		switch val.Field(i).Type().Kind() {
		case reflect.Uint8, reflect.Bool:
			{
				buf[pos] = byte(val.Field(i).Uint())
				pos = util.AdvanceCursor(pos, 1)
			}
		case reflect.Uint32:
			{
				binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(val.Field(i).Uint()))
				pos = util.AdvanceCursor(pos, 4)
			}
		}
	}

	return buf
}

func (header *nodeHeader) deserialize(bytes []byte) error {
	if len(bytes) != int(nodeHeaderSize()) {
		return fmt.Errorf("deserializing node header: invalid data length -- found %d, expected %d", len(bytes), nodeHeaderSize())
	}

	pos := 0
	header.Typ = NodeType(uint8(bytes[pos]))
	pos = util.AdvanceCursor(pos, 1)
	header.Parent = PageNum(binary.LittleEndian.Uint32(bytes[pos : pos+4]))
	pos = util.AdvanceCursor(pos, 4)
	header.Next = PageNum(binary.LittleEndian.Uint32(bytes[pos : pos+4]))
	pos = util.AdvanceCursor(pos, 4)
	header.CellSize = binary.LittleEndian.Uint32(bytes[pos : pos+4])
	pos = util.AdvanceCursor(pos, 4)
	header.Height = uint8(bytes[pos])
	pos = util.AdvanceCursor(pos, 1)
	header.NumCell = uint8(bytes[pos])

	return nil
}
