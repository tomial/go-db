package btree

// A simple b plus tree implementation
// order 3 example :
// 			    ┌────────┐
// 			    │   02   │
// 			    ├──┬──┬──┤
//     ┌────┤  │  │--│
//     │    └──┴─┬┴──┘
//     │         │
// 		 │         └────────┐
// ┌───▼────┐        ┌────▼────┐
// │   01   │        │ 02 │ 03 │
// ├──┬──┬──┤        ├──┬─┴─┬──┤
// │--│--│  ├───────►│--│ - │--│
// └──┴──┴──┘        └──┴───┴──┘

type NodeType uint8
type PageNum uint32

const (
	TypeInternal NodeType = iota
	TypeLeaf
)

type Node struct {
	Root     bool
	Typ      NodeType
	Parent   PageNum // Pointer to parent node
	Next     PageNum // Pointer to next leaf node
	NumCell  uint64  // Amount of cells inserted
	CellSize uint64  // Row Size
	Cell     []byte  // Rows
}
