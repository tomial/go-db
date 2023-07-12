package btree

import (
	"db/src/constants"
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
	// Headers: 10B
	Typ      NodeType // 1B
	Empty    bool
	Parent   PageNum // 4B Pointer to parent node (read actual struct with PageNum)
	Next     PageNum // 4B Pointer to next leaf node (-1(nil) for internal node)
	CellSize uint32  // 4B Size of node cell, The cell size of leaf node depends on what table(row) it stores
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

// 4082
func nodeBodySize() uint32 {
	return uint32(constants.PageSize) - nodeHeaderSize()
}
